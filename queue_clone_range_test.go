package eventqueue

import (
	"context"
	"errors"
	"sync"
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

type swappable[T any] struct {
	sync.Mutex
	blocker chan struct{}
	sink    Sink[T]
}

func newSwappable[T any]() *swappable[T] {
	return &swappable[T]{
		blocker: make(chan struct{}),
	}
}

func (s *swappable[T]) waitWrite() {
	s.blocker <- struct{}{}
}

func (s *swappable[T]) closeBlocker() {
	close(s.blocker)
}

func (s *swappable[T]) makeBlocker() {
	s.blocker = make(chan struct{})
}

func (s *swappable[T]) Write(ctx context.Context, e T) error {
	s.Lock()
	defer s.Unlock()
	<-s.blocker
	err := s.sink.Write(ctx, e)
	return err
}

func (s *swappable[T]) swap(o Sink[T]) Sink[T] {
	s.Lock()
	defer s.Unlock()
	before := s.sink
	s.sink = o
	return before
}

type errSink[T any] struct {
	Err error
}

func (s *errSink[T]) Write(ctx context.Context, e T) error {
	return s.Err
}

type sliceSink[T any] struct {
	Received []T
}

func (s *sliceSink[T]) Write(ctx context.Context, e T) error {
	s.Received = append(s.Received, e)
	return nil
}

func testEventQueue_Clone_Range[T any](t *testing.T, opts []Option[T], firstPush T, elements []T, expected []T) {
	sink := newSwappable[T]()
	_ = sink.swap(&errSink[T]{Err: errors.New("foo")})

	q := New[T](sink, append([]Option[T]{WithRetryInterval[T](time.Second)}, opts...)...)
	fakeClock := clockwork.NewFakeClock()
	q.clock = fakeClock

	ctx, cancel := context.WithCancel(context.Background())
	timingGroup := timing.NewGroup(ctx, false)

	timingGroup.Go(func(ctx context.Context) error {
		_, _ = q.Run(ctx)
		return nil
	})
	defer func() {
		cancel()
		_ = timingGroup.Wait()
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

	sink.makeBlocker()

	ranged := make([]T, 1+len(elements))
	q.Range(func(i int, e T) (next bool) {
		ranged[i] = e
		return true
	})

	cloned := q.Clone()
	_ = sink.swap(&sliceSink[T]{})
	sink.closeBlocker()

	fakeClock.Advance(2 * time.Second)

	q.Drain()

	slicer := sink.swap(&errSink[T]{}).(*sliceSink[T])

	received := slicer.Received

	assert.DeepEqual(t, expected, received)
	assert.DeepEqual(t, expected, ranged)
	assert.DeepEqual(t, expected, cloned)
}
