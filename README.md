# eventqueue

A generic event queue. By default it is an FIFO queue.

eventqueue queues up events (tasks) and send them to Sink.

## Custom queueing mechanism

Using `WithQueue` option with a `Queue[T]` implementor, You can swap internal queueing mechanism.

For default (w/o `WithQueue` option), `Deque[T any]`, a thin wrapper of [github.com/gammazero/deque](https://github.com/gammazero/deque), with initial capacity at 2^4 is used.

```go
func WithQueue[E any](queue Queue[E]) Option[E] {
	return func(q *EventQueue[E]) {
		q.queue = queue
	}
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
```

We also maintain a priority queue implementor.

```go
type rankedNum struct {
	Num  int
	Rank int
}

q := New(
	sink,
	WithQueue(NewPriorityQueue( // ranked num max heap.
		nil,
		func(i, j *rankedNum) bool {
			if i.Rank != j.Rank {
				return i.Rank > j.Rank
			}
			return i.Num > j.Num
		},
		priorityqueue.SliceInterfaceMethods[*rankedNum]{},
	)),
) // queued item sorted by its priority.
```

## Usage

eventqueue queues up event object E (whatever you want), and then write them into Sink once events are available.

Sink is an interface expressed as

```go
// Sink is written once EventQueue receives events.
// Write is serialized in EventQueue. It can be goroutine-unsafe method.
type Sink[E any] interface {
	// Write writes the event object to Sink.
	// If Write returns error, the event is put back to the head of queue.
	Write(ctx context.Context, event E) error
}
```

eventqueue can be `Push`-ed arbitrary number as long as the system has enough memory space.
It then tries to `Write` to `Sink` in FIFO order.

eventqueue also can `Reserve` happening of event after `fn func(context.Context) (E, error)`,
passed `fn` will be called in a new goroutine and once `fn` returns with nil error, returned event from `fn` enters eventqueue.

```go
func main() {
	// unbuffered channel sink.
	sink := eventqueue.NewChannelSink[int](0)
	// sink is interface.
	q := eventqueue.New[int](sink)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	// q also can Reserve happening of event after fn returns.
	// If fn returns with nil error, returned E enters queue.
	q.Reserve(func(ctx context.Context) (int, error) {
		time.Sleep(500 * time.Millisecond)
		return 999, nil
	})

	// q also can cancel too long fn.
	// Of course if and only if fn respects given ctx.
	q.Reserve(func(ctx context.Context) (int, error) {
		timer := time.NewTimer(time.Hour)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-timer.C:
			return 2999, nil
		}
	})

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		remaining, err := q.Run(ctx)
		fmt.Printf("tasks remaining in queue: %d, err = %v\n", remaining, err) // tasks remaining in queue: 0, err = <nil>
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 11; i++ {
			fmt.Printf("received: %d\n", <-sink.Outlet())
			/*
				received: 0
				received: 1
				received: 2
				received: 3
				received: 4
				received: 5
				received: 6
				received: 7
				received: 8
				received: 9
				received: 999
			*/
		}
		fmt.Println("done")
	}()

	// at this point
	queued, reserved := q.Len()
	fmt.Printf("queued = %d, reserved = %d\n", queued, reserved) // queued = 10, reserved = 2
	waitCh := q.WaitReserved()
	// can observe Reserved fn returned.
	<-waitCh

	// and now is
	queued, reserved = q.Len()
	fmt.Printf("queued = %d, reserved = %d\n", queued, reserved) // queued = 1, reserved = 1

	// requests cancellation.
	q.CancelReserved()
	for range waitCh {
	}
	// then
	queued, reserved = q.Len()
	fmt.Printf("queued = %d, reserved = %d\n", queued, reserved) // queued = 0, reserved = 0

	q.Drain()

	cancel()
	wg.Wait()
}
```
