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
)

func TestEventQueue_Clone_Range(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		testEventQueue_Clone_Range(
			t,
			[]Option[int]{},
			1,
			[]int{2, 3, 769, 1, 4, 658, 7, 2},
			[]int{1, 2, 3, 769, 1, 4, 658, 7, 2},
		)
	})
	t.Run("priority_queue", func(t *testing.T) {
		type rankedNum struct {
			Num  int
			Rank int
		}
		queue := NewPriorityQueue(
			nil,
			func(i, j *rankedNum) bool {
				if i.Rank != j.Rank {
					return i.Rank > j.Rank
				}
				return i.Num > j.Num
			},
			priorityqueue.SliceInterfaceMethods[*rankedNum]{},
		)
		testEventQueue_Clone_Range(
			t,
			[]Option[*rankedNum]{
				WithQueue(queue),
			},
			&rankedNum{Rank: 1, Num: 3},
			[]*rankedNum{{Rank: 3, Num: 9}, {Rank: 3, Num: 4}, {Rank: 999, Num: 3}},
			[]*rankedNum{{Rank: 1, Num: 3}, {Rank: 999, Num: 3}, {Rank: 3, Num: 9}, {Rank: 3, Num: 4}},
		)
	})
}

func testEventQueue_Clone_Range[T any](t *testing.T, opts []Option[T], firstPush T, elements []T, expected []T) {
	sink := newSwappable[T](t)
	_ = sink.swap(&errSink[T]{Err: errors.New("foo")})

	q := New[T](sink, append([]Option[T]{WithRetryInterval[T](time.Second)}, opts...)...)
	fakeClock := clockwork.NewFakeClock()
	q.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())
	g := timing.NewGroup(ctx, false)

	g.Go(func(ctx context.Context) error {
		_, _ = q.Run(ctx)
		return nil
	})
	defer func() {
		cancel()
		_ = g.Wait()
	}()

	waiter := timing.CreateWaiterCh(func() {
		// wait until q pushes back element to queue.
		q.WaitUntil(func(writing bool, queued, reserved int) bool {
			t.Logf("waiting: writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
			return writing == false && queued == 1
		})
	})

	q.Push(firstPush)
	sink.waitWrite()

	<-waiter

	waiter = timing.CreateWaiterCh(func() {
		// wait until q pushes back element to queue.
		q.WaitUntil(func(writing bool, queued, reserved int) bool {
			t.Logf("waiting: cond = %t, writing = %t, queued = %d, reserved = %d", writing == false && queued == 1+len(elements), writing, queued, reserved)
			return writing == false && queued == 1+len(elements)
		})
	})

	sink.closeBlocker()

	for _, e := range elements {
		q.Push(e)
	}

	<-waiter

	sink.Lock()
	sink.makeBlocker()
	sink.Unlock()

	q.WaitUntil(func(writing bool, queued, reserved int) bool {
		t.Logf("writing = %t, queued = %d, reserved = %d", writing, queued, reserved)
		if writing { // I dunno why but is flaky without this.
			sink.waitWrite()
		}
		return !writing
	})

	ranged := make([]T, 1+len(elements))
	q.Range(func(i int, e T) (next bool) {
		ranged[i] = e
		return true
	})

	cloned := q.Clone()
	channelSink := NewChannelSink[T](0)
	_ = sink.swap(channelSink)
	sink.closeBlocker()
	fakeClock.Advance(2 * time.Second)

	var received []T
	for {
		received = append(received, <-channelSink.Outlet())
		if len(received) == len(expected) {
			break
		}
	}

	q.Drain()

	assert.DeepEqual(t, expected, received)
	assert.DeepEqual(t, expected, ranged)
	assert.DeepEqual(t, expected, cloned)
}
