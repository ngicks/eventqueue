package eventqueue

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"log"

	"github.com/gammazero/deque"
	"github.com/ngicks/gommon/pkg/common"
)

// Sink is written once EventQueue receives events.
// Write is serialized in EventQueue. It can be goroutine-unsafe method.
type Sink[E any] interface {
	// Write writes the event object to Sink.
	// If Write returns error, the event is put back to the queue.
	Write(ctx context.Context, event E) error
}

type Option[E any] func(q *EventQueue[E])

// SetRetryInterval returns Option that sets the retry interval to q.
// Without this option, q does not retry to Write until another push event occurs.
func SetRetryInterval[E any](dur time.Duration) Option[E] {
	return func(q *EventQueue[E]) {
		q.dur = dur
	}
}

type EventQueue[E any] struct {
	queue *deque.Deque[E]
	sink  Sink[E]

	isRunning     bool
	hasUpdate     chan struct{}
	reserved      map[int]<-chan struct{}
	reservationId int

	cond         *sync.Cond
	dur          time.Duration
	timerFactory func() common.Timer
}

func New[E any](sink Sink[E], opts ...Option[E]) *EventQueue[E] {
	q := &EventQueue[E]{
		cond:         sync.NewCond(&sync.Mutex{}),
		sink:         sink,
		queue:        deque.New[E](10),
		hasUpdate:    make(chan struct{}, 1),
		reserved:     make(map[int]<-chan struct{}, 10),
		timerFactory: func() common.Timer { return common.NewTimerReal() },
	}

	for _, opt := range opts {
		opt(q)
	}

	return q
}

func (q *EventQueue[E]) IsRunning() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.isRunning
}

func (q *EventQueue[E]) Push(e E) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.queue.PushBack(e)

	select {
	case q.hasUpdate <- struct{}{}:
	default:
	}
}

// Reserve reserves update. fn will be called in a newly created goroutine.
func (q *EventQueue[E]) Reserve(fn func() E) {
	doneCh := make(chan struct{})

	q.cond.L.Lock()
	id := q.reservationId
	q.reservationId = rotatingAdd(q.reservationId)
	// id could overwrite existing entry.
	// It should not happen.
	q.reserved[id] = doneCh
	q.cond.L.Unlock()

	go func() {
		timer := q.timerFactory()

		timer.Reset(time.Hour)
		fnDone := make(chan struct{})
		var e E
		go func() {
			<-fnDone
			e = fn()
			close(fnDone)
		}()
		fnDone <- struct{}{}

		select {
		case <-fnDone:
			q.Push(e)
		case <-timer.C():
			log.Println(
				"[WARNING] github.com/ngicks/eventqueue: " +
					"Reserve timed out. The input fn blocked over 1 hour.",
			)
		}
		close(doneCh)

		q.cond.L.Lock()
		delete(q.reserved, id)
		q.cond.L.Unlock()
	}()
}

// WaitReserved returns a channel which is sent every time reserved event enters into the queue.
// The channel is closed once all reserved event,
// which was present at the moment WaitReserved is called,
// enter into the queue.
func (q *EventQueue[E]) WaitReserved() <-chan struct{} {
	q.cond.L.Lock()

	eventCh := make(chan struct{})
	wg := sync.WaitGroup{}
	for _, doneCh := range q.reserved {
		wg.Add(1)
		go func(doneCh <-chan struct{}) {
			<-doneCh
			eventCh <- struct{}{}
			wg.Done()
		}(doneCh)
	}

	q.cond.L.Unlock()

	go func() {
		wg.Wait()
		close(eventCh)
	}()

	return eventCh
}

func (q *EventQueue[E]) Len() (inQueue, reserved int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.queue.Len(), len(q.reserved)
}

// Drain blocks until queue and reserved events become 0.
func (q *EventQueue[E]) Drain() {
	q.cond.L.Lock()

	for {
		if q.queue.Len() == 0 && len(q.reserved) == 0 {
			break
		}
		q.cond.Wait()
	}
	q.cond.L.Unlock()
}

// Run runs q.
func (q *EventQueue[E]) Run(ctx context.Context) (remaining int, err error) {
	q.cond.L.Lock()
	if q.isRunning {
		q.cond.L.Unlock()
		return 0, errors.New("Run is called twice")
	}
	q.isRunning = true
	q.cond.L.Unlock()

	defer func() {
		q.cond.L.Lock()
		q.isRunning = false
		q.cond.L.Unlock()
	}()

	timer := q.timerFactory()
	stopTimer := func() {
		if !timer.Stop() {
			select {
			case <-timer.C():
			default:
			}
		}
	}
	resetTimer := func() {
		if q.dur > 0 {
			timer.Reset(q.dur)
		}
	}
	defer stopTimer()

	writeAll := func() {
		stopTimer()
		for {
			event, popped := q.pop()
			if !popped {
				break
			}

			err := q.sink.Write(ctx, event)

			if err != nil {
				q.cond.L.Lock()
				q.queue.PushFront(event)
				q.cond.L.Unlock()

				resetTimer()
				break
			}
		}
		q.cond.Broadcast()
	}

	len, _ := q.Len()

	if len > 0 {
		select {
		case q.hasUpdate <- struct{}{}:
		default:
		}
	}

	for {
		select {
		case <-ctx.Done():
			stopTimer()

			len, _ := q.Len()

			return len, nil
		case <-timer.C():
			writeAll()
		case <-q.hasUpdate:
			writeAll()
		}
	}
}

func (q *EventQueue[E]) pop() (event E, popped bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.queue.Len() > 0 {
		return q.queue.PopFront(), true
	} else {
		var zero E
		return zero, false
	}
}

func rotatingAdd(i int) int {
	if i != math.MaxInt {
		return i + 1
	}
	return math.MinInt
}
