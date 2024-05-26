package eventqueue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	timinghelper "github.com/ngicks/timing-helper"
	"github.com/stretchr/testify/assert"
)

type sink struct {
	err     error
	flowing bool
	blocker chan struct{} // blocker will be received twice per a Write call.
	written []int
	ctxCb   func(context.Context)
	mu      sync.Mutex
}

func newSink() *sink {
	return &sink{
		blocker: make(chan struct{}),
		written: make([]int, 0),
	}
}

func (s *sink) Write(ctx context.Context, e int) error {
	if !s.flowing {
		<-s.blocker
	}

	s.mu.Lock()

	s.written = append(s.written, e)
	err := s.err
	cb := s.ctxCb
	s.mu.Unlock()

	if cb != nil {
		cb(ctx)
	}

	if !s.flowing {
		<-s.blocker
	}

	return err
}

func TestEventQueue(t *testing.T) {
	assert := assert.New(t)

	sink := newSink()
	eventQueue := New[int](sink)

	ctx, cancel := context.WithCancel(context.Background())

	switchCh := make(chan struct{})
	go func() {
		<-switchCh
		rem, err := eventQueue.Run(ctx)
		assert.Equal(int(0), rem)
		assert.NoError(err)
		close(switchCh)
	}()
	switchCh <- struct{}{}

	rem, err := eventQueue.Run(context.Background())
	assert.Equal(int(0), rem)
	assert.Error(err)

	for i := 0; i < 5; i++ {
		eventQueue.Push(i)
	}

	for i := 0; i < 10; i++ {
		sink.blocker <- struct{}{}
	}

	sink.mu.Lock()
	assert.Len(sink.written, 5)
	for idx, e := range sink.written {
		assert.Equal(idx, e, "sink must receive Push-ed events in FIFO order.")
	}
	sink.written = sink.written[:0] // reset
	sink.mu.Unlock()

	blocker1 := make(chan struct{})
	blocker2 := make(chan struct{})
	blocker3 := make(chan struct{})
	eventQueue.Reserve(func(_ context.Context) (int, error) {
		<-blocker1
		return 1, nil
	})
	eventQueue.Reserve(func(_ context.Context) (int, error) {
		<-blocker2
		return 2, nil
	})
	eventQueue.Reserve(func(_ context.Context) (int, error) {
		<-blocker3
		return 3, nil
	})

	waiter := eventQueue.WaitReserved()

	select {
	case <-waiter:
		t.Error("The channel returned from WaitReserved must not receive when no reserved event is unblocked.")
	case sink.blocker <- struct{}{}:
		t.Error("Sink is written while no reserved event is unblocked.")
	case <-time.After(time.Microsecond):
	}

	for _, ch := range []chan struct{}{blocker2, blocker3, blocker1} {
		close(ch)
		sink.blocker <- struct{}{}
		sink.blocker <- struct{}{}
		_, ok := <-waiter
		assert.True(
			ok,
			"The channel returned from WaitReserved must not be closed"+
				" when there still are blocked reserved events.",
		)
	}

	_, ok := <-waiter
	assert.False(
		ok,
		"The channel returned from WaitReserved must be closed"+
			" when all reserved events are pushed.",
	)

	sink.mu.Lock()
	assert.Len(sink.written, 3)
	assert.Equal(2, sink.written[0], "The Push order of reserved tasks are first unblocked to last.")
	assert.Equal(3, sink.written[1], "The Push order of reserved tasks are first unblocked to last.")
	assert.Equal(1, sink.written[2], "The Push order of reserved tasks are first unblocked to last.")

	sink.written = sink.written[:0] // reset
	sink.mu.Unlock()

	cancel()
	<-switchCh
}

func TestEventQueue_timer_is_reset_when_sink_returns_error(t *testing.T) {
	assert := assert.New(t)

	sink := newSink()
	eventQueue := New[int](sink, WithRetryInterval[int](25))
	fakeClock := clockwork.NewFakeClock()
	eventQueue.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())

	var rem atomic.Int64 // to conform the race detector
	var err atomic.Pointer[error]
	switchCh := make(chan struct{})
	go func() {
		<-switchCh
		rem_, err_ := eventQueue.Run(ctx)
		rem.Store(int64(rem_))
		err.Store(&err_)
		close(switchCh)
	}()
	switchCh <- struct{}{}

	sampleErr := errors.New("sample")
	sink.mu.Lock()
	sink.err = sampleErr
	sink.mu.Unlock()

	eventQueue.Push(213)
	sink.blocker <- struct{}{}
	sink.blocker <- struct{}{}

	sink.mu.Lock()
	assert.Equal(213, sink.written[0], "Write of Sink must be called with Push-ed value.")
	sink.mu.Unlock()

	fakeClock.BlockUntil(1)
	fakeClock.Advance(30)
	sink.blocker <- struct{}{}
	sink.blocker <- struct{}{}

	sink.mu.Lock()
	assert.Equal(
		213, sink.written[1],
		"Write of Sink must be called again with "+
			"the same value as the one it has received when it returned error",
	)
	sink.mu.Unlock()

	for i := 0; i < 10; i++ {
		eventQueue.Push(i)
		sink.blocker <- struct{}{}
		sink.blocker <- struct{}{}
	}

	cancel()
	<-switchCh

	assert.Equal(int64(11), rem.Load())
	assert.NoError(*err.Load())

	sink.mu.Lock()
	sink.written = sink.written[:0] // reset
	sink.err = nil
	sink.mu.Unlock()

	ctx, cancel = context.WithCancel(context.Background())
	switchCh = make(chan struct{})
	go func() {
		<-switchCh
		rem_, err_ := eventQueue.Run(ctx)
		rem.Store(int64(rem_))
		err.Store(&err_)
		close(switchCh)
	}()
	switchCh <- struct{}{}

	// Run immediately Write-s events if it already has events in the queue.
	for i := 0; i < 11; i++ {
		sink.blocker <- struct{}{}
		sink.blocker <- struct{}{}
	}

	cancel()
	<-switchCh

	sink.mu.Lock()
	assert.Len(sink.written, 11)
	sink.mu.Unlock()
}

func TestEventQueue_cancelling_ctx(t *testing.T) {
	assert := assert.New(t)

	sink := newSink()
	eventQueue := New[int](sink, WithRetryInterval[int](25))

	eventQueue.Drain()

	ctx, cancel := context.WithCancel(context.Background())

	var rem atomic.Int64 // to conform the race detector
	var err atomic.Pointer[error]
	switchCh := make(chan struct{})
	go func() {
		<-switchCh
		rem_, err_ := eventQueue.Run(ctx)
		rem.Store(int64(rem_))
		err.Store(&err_)
		close(switchCh)
	}()
	switchCh <- struct{}{}

	called := make(chan struct{})
	sink.ctxCb = func(ctx context.Context) {
		<-called
		<-ctx.Done()
	}

	for i := 0; i < 10; i++ {
		eventQueue.Push(i)
	}

	sink.blocker <- struct{}{}
	inQ, reserved := eventQueue.Len()
	assert.Equal(9, inQ) // 10 pushed. 1 popped at the moment sink.blocker is received (no race condition).
	assert.Equal(0, reserved)
	sink.flowing = true

	called <- struct{}{}

	blocking := make(chan struct{})
	eventQueue.Reserve(func(ctx context.Context) (int, error) {
		<-blocking
		<-blocking
		return 0, nil
	})

	blocking <- struct{}{}

	inQ, reserved = eventQueue.Len()
	assert.Equal(9, inQ) // 10 pushed. 1 popped at the moment sink.blocker is received (no race condition).
	assert.Equal(1, reserved)

	drainWaiter := timinghelper.CreateWaiterCh(func() { eventQueue.Drain() })

	select {
	case <-drainWaiter:
		t.Error("Drain must not be unblocked at this moment")
	case <-time.After(time.Millisecond):
	}

	cancel()

	select {
	case <-switchCh:
		t.Errorf("Run must not return if Sink does not yet return (it is blocked intentionally).")
	case <-time.After(time.Millisecond):
	}

	close(called)

	<-switchCh

	close(blocking)
	for range eventQueue.WaitReserved() {
	}

	select {
	case <-drainWaiter:
		t.Error("Drain must not be unblocked at this moment." +
			" EventQueue is not running. Reserved element is no longer processed")
	case <-time.After(time.Millisecond):
	}

	sink.mu.Lock()
	assert.Len(sink.written, 10)
	sink.mu.Unlock()

	inQ, reserved = eventQueue.Len()
	assert.Equal(1, inQ)
	assert.Equal(0, reserved)

	// it should not block
	for range eventQueue.WaitReserved() {
	}

	sink.ctxCb = nil

	ctx, cancel = context.WithCancel(context.Background())
	runWaiter := timinghelper.CreateWaiterCh(func() { _, _ = eventQueue.Run(ctx) })

	<-drainWaiter
	cancel()
	<-runWaiter
}
