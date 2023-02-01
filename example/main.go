package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/ngicks/eventqueue"
)

func main() {
	sink := eventqueue.NewChannelSink[int]()
	q := eventqueue.New[int](sink)

	for i := 0; i < 10; i++ {
		q.Push(i)
	}

	var wg sync.WaitGroup

	ctx, cancel := context.WithCancel(context.Background())
	wg.Add(1)
	go func() {
		defer wg.Done()
		q.Run(ctx)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 10; i++ {
			fmt.Printf("received: %d\n", <-sink.Outlet())
		}
		fmt.Println("done")
	}()

	q.Drain()

	cancel()
	wg.Wait()
}
