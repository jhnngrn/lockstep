// Package lockstep implements a bilateral counter-based message protocol.
//
// Two peers share a counter. Each message occupies one counter slot.
// The response is an opaque byte — lockstep does not interpret it.
// Collision (both sides act at the same counter) is resolved by a
// deterministic turn bit derived from lexicographic key order.
//
// All public methods are safe for concurrent use. State is protected
// by a per-link mutex. The mutex is released during network sends
// so other operations on the link are not blocked.
package lockstep

import (
	"encoding/json"
	"fmt"
	"sync"
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
	mu     sync.Mutex
	closed bool

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
		self:    self,
		peer:    peer,
		turnBit: bit,
		handler: h,
		send:    send,
		persist: persist,
	}
}

// Close marks the link as closed. Ongoing sends will be discarded
// when they return.
func (l *Link) Close() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.closed = true
}

// dominant reports whether this side owns the current counter slot.
func (l *Link) dominant() bool {
	return l.Counter%2 == l.turnBit
}

// NewPendingTx tries to send the next queued transaction.
func (l *Link) NewPendingTx() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.tryNextTx()
	// tryNextTx -> sendPending handles unlock/relock around send.
	// After the chain completes, mu is held.
	l.mu.Unlock()
}

// Receive processes an incoming transaction.
// Returns (response, tsRejected, err).
func (l *Link) Receive(tx Tx) (response byte, tsRejected bool, err error) {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return 0, false, fmt.Errorf("link closed")
	}
	response, tsRejected, err = l.receive(tx)
	l.mu.Unlock()
	return
}

// --- Internal methods (called with mu held) ---

func (l *Link) tryNextTx() {
	if l.Counter == 0 || l.Pending != nil {
		return
	}

	payload, queuedAt := l.handler.NextTx(l.peer)
	if payload == nil {
		return
	}

	l.Pending = &PendingTx{Payload: payload, QueuedAt: queuedAt}
	l.Pending.Timestamp = maxTS(timeNow(), l.Pending.QueuedAt)
	l.persist()
	l.sendPending()
}

// sendPending sends the current in-flight message.
// Releases mu during the network send, retakes it after.
func (l *Link) sendPending() {
	tx := Tx{
		From:      l.self,
		To:        l.peer,
		Counter:   l.Counter,
		Payload:   l.Pending.Payload,
		Timestamp: l.Pending.Timestamp,
	}

	l.mu.Unlock()
	response, tsRejected, err := l.send(l.peer, tx)
	l.mu.Lock()

	if l.closed {
		return
	}

	// State may have changed while mu was released — a Receive
	// could have won a collision and cleared our Pending.
	if l.Pending == nil || l.Counter != tx.Counter {
		return
	}

	if err != nil {
		// Transport error — Pending survives with same timestamp.
		l.scheduleRetry()
		return
	}
	if tsRejected {
		l.Pending.Timestamp = maxTS(timeNow(), l.Pending.QueuedAt)
		l.persist()
		l.scheduleRetry()
		return
	}

	l.handler.HandleResponse(l.peer, l.Pending.Payload, response)
	l.AcceptResponse()
	l.tryNextTx()
}

// scheduleRetry schedules a retry of sendPending after RetryInterval.
func (l *Link) scheduleRetry() {
	counter := l.Counter
	time.AfterFunc(RetryInterval, func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		if l.closed {
			return
		}
		if l.Counter == counter {
			l.sendPending()
		}
	})
}

func (l *Link) receive(tx Tx) (response byte, tsRejected bool, err error) {
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
	l.tryNextTx()

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
	if l.Pending != nil && l.dominant() {
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
