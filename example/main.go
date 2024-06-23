package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ngicks/eventqueue"
)

func main() {
	// unbuffered channel sink.
	sink := eventqueue.NewChannelSink[int](0)
	// sink is interface.
	q := eventqueue.New[int](sink)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	go func() {
		for i := range 5 {
			q.Pusher() <- i + 50
		}
	}()

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
		for i := 0; i < 16; i++ {
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
				received: 50
				received: 51
				received: 52
				received: 53
				received: 54
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
