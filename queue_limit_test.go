package eventqueue

import (
	"context"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/ngicks/go-common/timing"
	"gotest.tools/v3/assert"
	"gotest.tools/v3/assert/cmp"
)

func TestEventQueue_size_limit(t *testing.T) {
	sink := newSwappable[int]()
	_ = sink.swap(&errSink[int]{Err: errors.New("foo")})

	q := New(sink, WithQueueSize[int](10))

	for i := range 10 {
		q.Push(i)
	}

	pushUnblocked := timing.CreateWaiterCh(func() { q.Push(10) })

	select {
	case <-time.NewTimer(time.Millisecond).C:
	case <-pushUnblocked:
		t.Fatal("push unblocked; ignoring queue size")
	}

	queued, reserved := q.Len()
	assert.Equal(t, queued, 10)
	assert.Equal(t, reserved, 0)

	// Reserve ignores size limit.
	q.Reserve(func(ctx context.Context) (int, error) {
		return 11, nil
	})
	q.Reserve(func(ctx context.Context) (int, error) {
		return 12, nil
	})

	for range q.WaitReserved() {
	}

	queued, reserved = q.Len()
	assert.Equal(t, queued, 12)
	assert.Equal(t, reserved, 0)

	ctx, cancel := context.WithCancel(context.Background())
	g := timing.NewGroup(ctx, false)
	g.Go(func(ctx context.Context) error {
		_, err := q.Run(ctx)
		return err
	})

	pusherChanUnblocked := timing.CreateWaiterCh(func() { q.Pusher() <- 13 })

	select {
	case <-time.NewTimer(time.Millisecond).C:
	case <-pushUnblocked:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pusherChanUnblocked:
		t.Fatal("sending on Pusher channel unblocked; ignoring queue size")
	}

	sink.waitWrite()
	var received []int
	chanSink := NewChannelSink[int](0)
	_ = sink.swap(chanSink)

	g.Go(func(ctx context.Context) error {
		q.WaitUntil(func(writing bool, queued, reserved int) bool {
			t.Logf("writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
			return ctx.Err() != nil
		})
		return nil
	})

	// Write error stops q.
	// The main cause of Write error is assumed a context cancellation.
	// Therefore automatic retry is considered unnatural.
	q.WaitUntil(func(writing bool, queued, reserved int) bool {
		t.Logf("writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
		return !writing && queued == 12 // 2 is blocked on.
	})

	// Pushing it signals update anyway
	pushUnblocked2 := timing.CreateWaiterCh(func() { q.Push(14) })

	t.Logf("let single write unblocked and succeed")
	sink.waitWrite() // let single Write succeed.
	received = append(received, <-chanSink.Outlet())

	select {
	case <-time.NewTimer(time.Millisecond).C:
	case <-pushUnblocked:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pushUnblocked2:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pusherChanUnblocked:
		t.Fatal("sending on Pusher channel unblocked; ignoring queue size")
	}

	sink.waitWrite() // synchronized at Write is called, keep it blocked on Write.
	received = append(received, <-chanSink.Outlet())

	select {
	case <-time.NewTimer(time.Millisecond).C:
		time.Sleep(time.Millisecond)
		queued, _ := q.Len()
		t.Fatalf("timed out; expected single push attempt unblocks: queued = %d", queued)
	case <-pushUnblocked:
		pushUnblocked = nil
	case <-pushUnblocked2:
		pushUnblocked2 = nil
	case <-pusherChanUnblocked:
		pusherChanUnblocked = nil
	}

	sink.waitWrite()
	q.WaitUntil(func(writing bool, queued, reserved int) bool {
		t.Logf("writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
		return writing && queued == 10
	})

	queued, reserved = q.Len()
	assert.Equal(t, queued, 10)
	assert.Equal(t, reserved, 0)

	pushUnblocked3 := timing.CreateWaiterCh(func() { q.Push(15) })
	pusherChanUnblocked2 := timing.CreateWaiterCh(func() { q.Pusher() <- 16 })

	select {
	case <-time.NewTimer(time.Millisecond).C:
	case <-pushUnblocked:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pushUnblocked2:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pushUnblocked3:
		t.Fatal("push unblocked; ignoring queue size")
	case <-pusherChanUnblocked:
		t.Fatal("sending on Pusher channel unblocked; ignoring queue size")
	case <-pusherChanUnblocked2:
		t.Fatal("sending on Pusher channel unblocked; ignoring queue size")
	}

	queued, reserved = q.Len()
	assert.Equal(t, queued, 10)
	assert.Equal(t, reserved, 0)

	g.Go(func(ctx context.Context) error {
		for {
			select {
			case <-ctx.Done():
				return nil
			case e := <-chanSink.Outlet():
				t.Logf("received: %d", e)
				received = append(received, e)
			}
		}
	})

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case sink.blocker <- struct{}{}:
			}
		}
	}()

	for range 4 { // one channel already closed
		select {
		case <-pushUnblocked:
			pushUnblocked = nil
		case <-pushUnblocked2:
			pushUnblocked2 = nil
		case <-pushUnblocked3:
			pushUnblocked3 = nil
		case <-pusherChanUnblocked:
			pusherChanUnblocked = nil
		case <-pusherChanUnblocked2:
			pusherChanUnblocked2 = nil
		}
	}
	assert.Assert(
		t,
		cmp.Len(
			slices.DeleteFunc(
				[]<-chan struct{}{
					pushUnblocked,
					pushUnblocked2,
					pushUnblocked3,
					pusherChanUnblocked,
					pusherChanUnblocked2,
				},
				func(e <-chan struct{}) bool { return e == nil },
			),
			0,
		),
	)

	q.WaitUntil(func(writing bool, queued, reserved int) bool {
		t.Logf("writing = %t, queued = %d, reserved =  %d", writing, queued, reserved)
		return writing == false && queued == 0 && reserved == 0
	})

	cancel()
	assert.NilError(t, g.Wait())

	assert.Assert(
		t,
		contains0toN(received, len(received)),
		"assumed to be slices that contains 0 to %d, but is %#v",
		len(received), received,
	)
}

func contains0toN(nums []int, n int) bool {
	for i := range n {
		if !slices.Contains(nums, i) {
			return false
		}
	}
	return true
}
