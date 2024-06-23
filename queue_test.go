package eventqueue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/ngicks/go-common/generic/priorityqueue"
	"github.com/ngicks/go-common/timing"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func TestEventQueue(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testEventQueue(t)
	})
	t.Run("priority_queue", func(t *testing.T) {
		testEventQueue(t, WithQueue(NewPriorityQueue(
			nil,
			func(i, j int) bool { return i < j },
			priorityqueue.SliceInterfaceMethods[int]{},
		)))
	})
}

func testEventQueue(t *testing.T, opts ...Option[int]) {
	sink := newSwappable[int](t)
	channelSink := NewChannelSink[int](0)
	_ = sink.swap(channelSink)

	q := New(sink, opts...)

	ctx, cancel := context.WithCancel(context.Background())
	g := timing.NewGroup(ctx, false)

	g.Go(func(ctx context.Context) error {
		remaining, err := q.Run(ctx)
		assert.Equal(t, remaining, 0)
		return err
	})

	rem, err := q.Run(context.Background())
	assert.Equal(t, int(0), rem)
	assert.ErrorIs(t, err, ErrAlreadyRunning)

	for i := range 5 {
		q.Push(i)
	}

	go func() {
		for range 5 {
			sink.waitWrite()
		}
	}()

	for i := range 5 {
		received := <-channelSink.Outlet()
		assert.Equal(t, received, i)
	}

	blocker1 := make(chan struct{})
	blocker2 := make(chan struct{})
	blocker3 := make(chan struct{})
	q.Reserve(func(_ context.Context) (int, error) {
		<-blocker1
		return 1, nil
	})
	q.Reserve(func(_ context.Context) (int, error) {
		<-blocker2
		return 2, nil
	})
	q.Reserve(func(_ context.Context) (int, error) {
		<-blocker3
		return 3, nil
	})

	waiter := q.WaitReserved()

	select {
	case <-waiter:
		t.Error("The channel returned from WaitReserved must not receive when no reserved event is unblocked.")
	case sink.blocker <- struct{}{}:
		t.Error("Sink is written while no reserved event is unblocked.")
	case <-time.After(time.Microsecond):
	}

	var received []int

	for _, ch := range []chan struct{}{blocker2, blocker3, blocker1} {
		close(ch)
		sink.waitWrite()
		received = append(received, <-channelSink.Outlet())
		_, ok := <-waiter
		assert.Assert(
			t,
			ok,
			"The channel returned from WaitReserved must not be closed"+
				" when there still are blocked reserved events.",
		)
	}

	_, ok := <-waiter
	assert.Assert(
		t,
		!ok,
		"The channel returned from WaitReserved must be closed"+
			" when all reserved events are pushed.",
	)

	assert.Assert(t, cmp.Len(received, 3))
	assert.Equal(t, 2, received[0], "The Push order of reserved tasks are first unblocked to last.")
	assert.Equal(t, 3, received[1], "The Push order of reserved tasks are first unblocked to last.")
	assert.Equal(t, 1, received[2], "The Push order of reserved tasks are first unblocked to last.")

	cancel()
	assert.NilError(t, g.Wait())
}

func TestEventQueue_timer_is_reset_when_sink_returns_error(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testEventQueue_timer_is_reset_when_sink_returns_error(t)
	})
	t.Run("priority_queue", func(t *testing.T) {
		testEventQueue_timer_is_reset_when_sink_returns_error(t, WithQueue(NewPriorityQueue(
			nil,
			func(i, j int) bool { return i < j },
			priorityqueue.SliceInterfaceMethods[int]{},
		)))
	})
}

func testEventQueue_timer_is_reset_when_sink_returns_error(t *testing.T, opts ...Option[int]) {
	sink := newSwappable[int](t)

	sampleErr := errors.New("sample")
	_ = sink.swap(&errSink[int]{sampleErr})

	q := New(sink, append([]Option[int]{WithRetryInterval[int](time.Hour)}, opts...)...)
	fakeClock := clockwork.NewFakeClock()
	q.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())
	g := timing.NewGroup(ctx, false)
	defer func() { _ = g.Wait() }()
	defer cancel()

	g.Go(func(ctx context.Context) error {
		_, _ = q.Run(ctx)
		return nil
	})

	q.Push(213)
	sink.waitWrite()
	q.WaitUntil(func(writing bool, queued, reserved int) bool {
		t.Logf("writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
		if writing {
			sink.waitWrite() // TODO: why is this needed?
		}
		return !writing
	})

	_ = sink.swap(&sliceSink[int]{})

	fakeClock.BlockUntil(1)
	fakeClock.Advance(time.Hour + 30*time.Minute)

	sink.waitWrite()

	pushed := sink.swap(&errSink[int]{}).(*sliceSink[int]).Received[0]

	assert.Equal(
		t,
		213, pushed,
		"Write of Sink must be called again with "+
			"the same value as the one it has received when it returned error",
	)
}

func TestEventQueue_cancelling_ctx(t *testing.T) {
	sink := newSwappable[int](t)
	channelSink := NewChannelSink[int](0)
	_ = sink.swap(channelSink)
	q := New[int](sink, WithRetryInterval[int](25))

	ctx, cancel := context.WithCancel(context.Background())

	g := timing.NewGroup(ctx, false)
	var (
		rem  int
		err  error
		done = make(chan struct{})
	)
	g.Go(func(ctx context.Context) error {
		rem, err = q.Run(ctx)
		close(done)
		return nil
	})

	for i := range 10 {
		q.Push(i)
	}

	sink.waitWrite()
	queued, reserved := q.Len()
	assert.Equal(t, 9, queued)
	assert.Equal(t, 0, reserved)
	<-channelSink.Outlet()

	q.WaitUntil(func(writing bool, queued, reserved int) bool { return writing && queued == 8 }) // should be blocked on swappable channel

	queued, reserved = q.Len()
	assert.Equal(t, 8, queued)
	assert.Equal(t, 0, reserved)

	cancel()

	select {
	case <-done:
		t.Fatal("Run must not unblock")
	case <-time.NewTimer(time.Millisecond).C:
	}

	sink.closeBlocker()
	_ = g.Wait()
	assert.Equal(t, rem, 9) // nil error pushed element back.
	assert.NilError(t, err)
}
