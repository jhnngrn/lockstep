// Package lockstep implements a bilateral counter-based message protocol.
//
// Two peers share a counter. Each message occupies one counter slot.
// The response is an opaque byte — lockstep does not interpret it.
// Collision (both sides act at the same counter) is resolved by a
// deterministic turn bit derived from lexicographic key order.
//
// The Link drives the send loop itself: call TryNextTx after queuing work,
// and lockstep pulls the next message via Handler.NextTx, sends it,
// processes the response, and kicks again until the queue is empty.
package lockstep

import (
	"encoding/json"
	"fmt"
	"time"
)

// MaxTimestampDrift is the maximum allowed clock difference (10 minutes).
const MaxTimestampDrift = 10 * 60

// RetryInterval is the delay between send retries on transport error.
var RetryInterval = time.Minute

// timeNow returns the current unix timestamp. Replaceable in tests.
var timeNow = func() int64 { return time.Now().Unix() }

// Tx is the wire envelope.
type Tx struct {
	From      string          `json:"from"`
	To        string          `json:"to"`
	Counter   uint64          `json:"counter"`
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
}

// PendingTx is the transaction currently in-flight.
type PendingTx struct {
	Payload   json.RawMessage `json:"payload"`
	Timestamp int64           `json:"timestamp"`
	QueuedAt  int64           `json:"queued_at"`
}

// CachedResponse stores the last response for replay on duplicate counter.
type CachedResponse struct {
	Response byte `json:"response"`
}

// Handler processes incoming messages and produces the next outgoing message.
// All payloads and responses are opaque to lockstep.
type Handler interface {
	// HandleRequest processes an incoming message.
	// Returns the response byte.
	HandleRequest(peer string, payload json.RawMessage, ts int64) byte

	// HandleResponse processes the response to our sent message.
	HandleResponse(peer string, payload json.RawMessage, response byte)

	// NextTx returns the next transaction to send, or nil if nothing queued.
	// queuedAt is used as a minimum timestamp: lockstep sends with
	// max(now, queuedAt) to handle backward clock jumps.
	NextTx(peer string) (payload json.RawMessage, queuedAt int64)
}

// SendFunc transmits a Tx to a peer and returns the response.
// tsRejected=true means the receiver rejected the timestamp (retry).
type SendFunc func(peer string, tx Tx) (response byte, tsRejected bool, err error)

// Link is a bilateral lockstep connection between two peers.
type Link struct {
	self    string
	peer    string
	Counter uint64 `json:"counter"` // 0 = pending connect, >= 1 = connected
	turnBit uint64 // derived from self > peer

	Pending        *PendingTx      `json:"pending,omitempty"`
	CachedResponse *CachedResponse `json:"cached_response,omitempty"`
	Timestamp      int64           `json:"timestamp"`

	handler Handler
	send    SendFunc
	persist func()
}

// NewLink creates a lockstep link to a peer.
func NewLink(self, peer string, h Handler, send SendFunc, persist func()) *Link {
	var bit uint64
	if self > peer {
		bit = 1
	}
	return &Link{
		self:          self,
		peer:          peer,
		turnBit:       bit,
		handler: h,
		send:          send,
		persist:       persist,
	}
}

// Peer returns the peer's identity.
func (l *Link) Peer() string { return l.peer }

// Dominant reports whether this side owns the current counter slot.
func (l *Link) Dominant() bool {
	return l.Counter%2 == l.turnBit
}

// TryNextTx pulls the next transaction from the handler and sends it.
// Call after queuing work in the handler.
func (l *Link) TryNextTx() {
	if l.Counter == 0 || l.Pending != nil {
		return
	}

	payload, queuedAt := l.handler.NextTx(l.peer)
	if payload == nil {
		return
	}

	l.Pending = &PendingTx{Payload: payload, QueuedAt: queuedAt}
	l.sendPending()
}

// sendPending sends the current in-flight message.
// Updates timestamp and persists before each attempt.
func (l *Link) sendPending() {
	l.Pending.Timestamp = maxTS(timeNow(), l.Pending.QueuedAt)
	l.persist()

	tx := Tx{
		From:      l.self,
		To:        l.peer,
		Counter:   l.Counter,
		Payload:   l.Pending.Payload,
		Timestamp: l.Pending.Timestamp,
	}

	response, tsRejected, err := l.send(l.peer, tx)
	if err != nil {
		// Transport error — Pending survives. Retry after interval.
		time.AfterFunc(RetryInterval, func() {
			l.sendPending()
		})
		return
	}
	if tsRejected {
		l.sendPending()
		return
	}

	l.handler.HandleResponse(l.peer, l.Pending.Payload, response)
	l.AcceptResponse()
	l.TryNextTx()
}

// Receive processes an incoming transaction.
// Returns (response, tsRejected, err).
func (l *Link) Receive(tx Tx) (response byte, tsRejected bool, err error) {
	// Validate counter.
	if cached, err := l.ValidateCounter(tx.Counter); err != nil {
		return 0, false, err
	} else if cached != nil {
		return cached.Response, false, nil
	}

	// Validate timestamp. Counter does not advance — sender retries.
	if !l.ValidateTimestamp(tx.Timestamp) {
		return 0, true, nil
	}

	// Dispatch to handler.
	response = l.handler.HandleRequest(l.peer, tx.Payload, tx.Timestamp)
	l.AcceptRequest(response, tx.Timestamp)
	l.TryNextTx()

	return response, false, nil
}

// --- Counter and timestamp validation ---

// ValidateCounter checks whether an incoming message at the given counter
// is valid. Returns a cached response for replayed counters.
func (l *Link) ValidateCounter(counter uint64) (cached *CachedResponse, err error) {
	if counter < l.Counter {
		if l.CachedResponse != nil && counter == l.Counter-1 {
			return l.CachedResponse, nil
		}
		return nil, fmt.Errorf("stale counter: got %d, at %d", counter, l.Counter)
	}
	if counter > l.Counter {
		return nil, fmt.Errorf("counter ahead: got %d, at %d", counter, l.Counter)
	}
	// Collision: both sides acting at the same counter.
	if l.Pending != nil && l.Dominant() {
		return nil, fmt.Errorf("collision: our pending wins")
	}
	return nil, nil
}

// ValidateTimestamp checks that ts is within acceptable bounds.
func (l *Link) ValidateTimestamp(ts int64) bool {
	now := timeNow()
	if ts < l.Timestamp {
		return false
	}
	diff := ts - now
	if diff < 0 {
		diff = -diff
	}
	return diff <= MaxTimestampDrift
}

// --- State transitions ---

// AcceptRequest finalizes an incoming request and persists.
// Clears Pending — if we had one, it lost the collision.
func (l *Link) AcceptRequest(response byte, ts int64) {
	l.CachedResponse = &CachedResponse{Response: response}
	l.Counter++
	l.Timestamp = ts
	l.Pending = nil
	l.persist()
}

// AcceptResponse finalizes a received response and persists.
func (l *Link) AcceptResponse() {
	if l.Pending == nil {
		return
	}
	l.Timestamp = l.Pending.Timestamp
	l.Counter++
	l.Pending = nil
	l.CachedResponse = nil
	l.persist()
}

func maxTS(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

