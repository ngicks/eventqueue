# eventqueue

A generic FIFO event queue.

eventqueue queues up events (tasks) and send them to Sink in FIFO order.

## Usage

```go
// unbuffered channel sink.
sink := eventqueue.NewChannelSink[int](0)
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
```
