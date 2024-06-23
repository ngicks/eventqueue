package eventqueue

import (
	"context"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jonboulle/clockwork"
)

var (
	ErrAlreadyRunning = errors.New("already running")
	ErrClosed         = errors.New("closed")
)

// Sink is written once EventQueue receives events.
// Write is serialized in EventQueue. It can be goroutine-unsafe method.
type Sink[E any] interface {
	// Write writes the event object to Sink.
	// If Write returns error, the event is put back to head of the queue,
	// which does not make q.Run return with that error,
	// only suspends q until next Push or after retry timeout.
	Write(ctx context.Context, event E) error
}

type Queue[T any] interface {
	Range(fn func(i int, e T) (next bool))
	Clone() []T
	Clear()
	Len() int
	PopFront() T
	PushBack(elem T)
	PushFront(elem T)
}

type reservation struct {
	done   <-chan struct{}
	cancel func()
}

type qState struct {
	stopping bool
	writing  bool
	queued   int
	reserved int
}

func newQState[T any](q *EventQueue[T]) qState {
	return qState{
		q.stopping, q.writing, q.queue.Len(), len(q.reserved),
	}
}

type EventQueue[E any] struct {
	queue Queue[E]
	sink  Sink[E]

	isRunning          atomic.Bool
	stopping           bool
	writing            bool
	hasUpdate          chan struct{}
	reserved           map[int]reservation
	reservationId      int
	reservationTimeout time.Duration

	pusher      chan E
	limitNotice chan struct{} // notice q that
	queueSize   int

	mu                    sync.Mutex
	drainNotifier         chan struct{}
	stateChangeNotifier   map[int]chan qState
	stateChangeNotifierId int
	retryTimeout          time.Duration
	clock                 clockwork.Clock
}

func New[E any](sink Sink[E], opts ...Option[E]) *EventQueue[E] {
	q := &EventQueue[E]{
		drainNotifier:       make(chan struct{}),
		stateChangeNotifier: make(map[int]chan qState),
		sink:                sink,
		hasUpdate:           make(chan struct{}, 1),
		reserved:            make(map[int]reservation, 1<<4),
		pusher:              make(chan E),
		limitNotice:         make(chan struct{}, 1),
		clock:               clockwork.NewRealClock(),
	}

	for _, opt := range opts {
		opt(q)
	}

	if q.queue == nil {
		q.queue = NewDeque[E](1 << 4)
	}

	return q
}

func (q *EventQueue[E]) announcedChangeLocked(f func(q *EventQueue[E])) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.announcedChange(f)
}

func (q *EventQueue[E]) announcedChange(f func(q *EventQueue[E])) {
	f(q)
	state := newQState(q)
	for _, c := range q.stateChangeNotifier {
		select {
		case c <- state:
		default:
		}
	}
}

func (q *EventQueue[E]) IsRunning() bool {
	return q.isRunning.Load()
}

func (q *EventQueue[E]) Pusher() chan<- E {
	return q.pusher
}

func (q *EventQueue[E]) Push(e E) {
	q.push(e, true)
}

func (q *EventQueue[E]) push(e E, block bool) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if block {
		select {
		case q.hasUpdate <- struct{}{}:
		default:
		} // notice update anyway
		_ = q.waitQueueRoom(context.Background(), false)
	}

	q.announcedChange(func(q *EventQueue[E]) {
		q.queue.PushBack(e)
	})
	if q.queueSize > 0 && q.queue.Len() >= q.queueSize { // Reservations are unbound. They'll exceeds the limit.
		select {
		case q.limitNotice <- struct{}{}:
		default:
		}
	}

	select {
	case q.hasUpdate <- struct{}{}:
	default:
	}

}

func (q *EventQueue[E]) waitQueueRoom(ctx context.Context, unblockOnStopping bool) error {
	if q.queueSize > 0 && q.queue.Len() >= q.queueSize {
		return q.waitUntil(ctx, 10, true, func(stopping, writing bool, queued, reserved int) bool {
			if unblockOnStopping && stopping {
				return true
			}
			return queued < q.queueSize
		})
	}
	return nil
}

// Range calls fn sequentially for each element in q. If fn returns false, range stops the iteration.
// The order of elements always is same as what the Sink would see them.
func (q *EventQueue[E]) Range(fn func(i int, e E) (next bool)) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queue.Range(fn)
}

// Clone clones internal contents of q.
// The order of elements always is same as what the Sink would see them.
//
// Calling Clone on running q might be wrong choice since it would block long if q holds many elements.
// An element being sent through Sink.Write may not be included in returned slice.
// If Sink.Write failed, the element would be pushed back to the head of q.
// So any subsequent calls could observe an additional element on head.
func (q *EventQueue[E]) Clone() []E {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Clone()
}

// Clear clears q.
// It may or may not retain memory allocated for q.
// Calling Clear on running q might be wrong.
func (q *EventQueue[E]) Clear() {
	q.announcedChangeLocked(func(q *EventQueue[E]) {
		q.queue.Clear()
	})
}

// CancelReserved cancels all jobs reserved via Reserve.
// CancelReserved only cancels all reservations present at the time CancelReserved is called.
// q is still valid and usable after this method returns.
func (q *EventQueue[E]) CancelReserved() {
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, reservation := range q.reserved {
		reservation.cancel()
	}
}

// Reserve reserves an update which will occur after fn returns.
//
// fn will be called in a newly created goroutine,
// and it must return E.
// It also must respect ctx cancellation whose cause will be ErrClosed in case it has been cancelled.
// Cancellation would only happen if CancelReserved was called.
//
// E returned by fn enters q only and only if it returned nil error.
func (q *EventQueue[E]) Reserve(fn func(context.Context) (E, error)) {
	doneCh := make(chan struct{})
	var (
		ctx    context.Context
		cancel func()
	)
	if q.reservationTimeout > 0 {
		ctx, cancel = context.WithTimeoutCause(context.Background(), q.reservationTimeout, ErrClosed)
	} else {
		ctx2, cancel2 := context.WithCancelCause(context.Background())
		ctx = ctx2
		cancel = func() { cancel2(ErrClosed) }
	}
	q.mu.Lock()
	id := q.reservationId
	q.reservationId = rotatingAdd(q.reservationId)
	// id could overwrite existing entry.
	// Not likely but possible.
	// TODO: detect overwrite and panic if any?
	q.announcedChange(func(q *EventQueue[E]) {
		q.reserved[id] = reservation{doneCh, cancel}
	})
	q.mu.Unlock()

	go func() {
		e, err := fn(ctx)
		if err == nil {
			q.push(e, false)
		}
		close(doneCh)
		q.announcedChangeLocked(func(q *EventQueue[E]) {
			delete(q.reserved, id)
		})
	}()
}

// WaitReserved returns a channel which receives
// every time reserved event enters into the queue, or has been cancelled.
//
// The channel is closed once all reservation events,
// which was present at the moment WaitReserved is called,
// are done.
func (q *EventQueue[E]) WaitReserved() <-chan struct{} {
	q.mu.Lock()

	eventCh := make(chan struct{})
	var wg sync.WaitGroup
	for _, reservation := range q.reserved {
		wg.Add(1)
		go func(doneCh <-chan struct{}) {
			defer wg.Done()
			<-doneCh
			eventCh <- struct{}{}
		}(reservation.done)
	}

	q.mu.Unlock()

	go func() {
		wg.Wait()
		close(eventCh)
	}()

	return eventCh
}

func (q *EventQueue[E]) Len() (queued, reserved int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.queue.Len(), len(q.reserved)
}

// Drain blocks until queue and reserved events become 0.
func (q *EventQueue[E]) Drain() {
	q.mu.Lock()
	defer q.mu.Unlock()
	_ = q.waitUntil(context.Background(), 100, false, func(stopping, writing bool, queued, reserved int) bool {
		return !writing && queued == 0 && reserved == 0
	})
}

func (q *EventQueue[E]) WaitUntil(cond func(writing bool, queued, reserved int) bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_ = q.waitUntil(context.Background(), 100, false, func(stopping, writing bool, queued, reserved int) bool {
		return cond(writing, queued, reserved)
	})
}

func (q *EventQueue[E]) waitUntil(ctx context.Context, bufSize int, recheck bool, cond func(stopping, writing bool, queued, reserved int) bool) error {
	state := newQState(q)
	if cond(state.stopping, state.writing, state.queued, state.reserved) {
		// stay locked, unlocking is caller's responsibility.
		return nil
	}

	c := make(chan qState, bufSize)
	nextIdx := q.stateChangeNotifierId
	q.stateChangeNotifierId = rotatingAdd(q.stateChangeNotifierId)
	q.stateChangeNotifier[nextIdx] = c

	q.mu.Unlock() // unlock, wait for events to come.

	defer func() {
		// at the return it must be locked.
		delete(q.stateChangeNotifier, nextIdx)
	}()

L:
	for {
		select {
		case <-ctx.Done():
			q.mu.Lock()
			return ctx.Err()
		case state := <-c:
			// fmt.Printf("state: %#v\n", state)
			if !cond(state.stopping, state.writing, state.queued, state.reserved) {
				continue L
			}
			q.mu.Lock()
			if !recheck {
				// locked
				return nil
			}
			state = newQState(q)
			if !cond(state.stopping, state.writing, state.queued, state.reserved) {
				q.mu.Unlock() // unlock, continue waiting.
				continue L
			}
			// locked
			return nil
		}
	}
}

// Run runs q.
// It blocks until ctx is cancelled.
func (q *EventQueue[E]) Run(ctx context.Context) (remaining int, err error) {
	if !q.isRunning.CompareAndSwap(false, true) {
		return 0, ErrAlreadyRunning
	}
	defer func() {
		q.announcedChangeLocked(func(q *EventQueue[E]) {
			q.stopping = false
		})
		q.isRunning.Store(false)
	}()

	retryTimer := q.clock.NewTimer(30 * 24 * time.Hour) // far future.
	_ = retryTimer.Stop()

	var set bool
	resetTimer := func() {}
	if q.retryTimeout > 0 {
		resetTimer = func() {
			retryTimer.Reset(q.retryTimeout)
			set = true
		}
	}
	stopTimer := func() {
		if !retryTimer.Stop() && set {
			// This is too difficult to use correctly.
			// It states
			//
			// > https://pkg.go.dev/time@go1.22.3#Timer.Stop
			// > It returns true if the call stops the timer, false if the timer has already expired or been stopped.
			//
			// Without an external flag, we have no clue to know if it has been fired or simply not yet reset.
			// Blocking on channel without doubts are too dangerous,
			// which could cause blocking forever.
			<-retryTimer.Chan()
		}
		set = false
	}

	defer stopTimer()

	writeAll := func() {
		stopTimer()
		for {
			event, popped := q.pop()
			if !popped {
				break
			}

			q.announcedChangeLocked(func(q *EventQueue[E]) {
				q.writing = true
			})

			err := q.sink.Write(ctx, event)

			q.announcedChangeLocked(func(q *EventQueue[E]) {
				q.writing = false
				if err != nil {
					q.queue.PushFront(event)
				}
			})

			if err != nil {
				resetTimer()
				break
			}
		}
	}

	len, _ := q.Len()

	if len > 0 {
		select {
		case q.hasUpdate <- struct{}{}:
		default:
		}
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.pusherLoop(ctx)
	}()

	for {
		select {
		case <-ctx.Done():
			stopTimer()

			q.announcedChangeLocked(func(q *EventQueue[E]) {
				q.stopping = true
			})

			len, _ := q.Len()

			return len, nil
		case <-retryTimer.Chan():
			set = false
			writeAll()
		case <-q.hasUpdate:
			writeAll()
		}
	}
}

func (q *EventQueue[E]) pusherLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-q.limitNotice:
			q.mu.Lock()
			_ = q.waitQueueRoom(ctx, true)
			q.mu.Unlock()
		default:
			select {
			case <-ctx.Done():
				return
			case <-q.limitNotice:
				q.mu.Lock()
				_ = q.waitQueueRoom(ctx, true)
				q.mu.Unlock()
			case e, ok := <-q.pusher:
				if !ok {
					panic("EventQueue[E]: Pusher is closed")
				}
				q.mu.Lock()
				q.announcedChange(func(q *EventQueue[E]) {
					q.queue.PushBack(e)
				})
				_ = q.waitQueueRoom(ctx, true)
				q.mu.Unlock()
			}
		}
	}
}

func (q *EventQueue[E]) pop() (event E, popped bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
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
