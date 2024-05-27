package eventqueue

import "context"

type ChannelSink[E any] struct {
	ch chan E
}

func NewChannelSink[E any](buf uint) *ChannelSink[E] {
	return &ChannelSink[E]{
		ch: make(chan E, int(buf)),
	}
}

func (s *ChannelSink[E]) Write(ctx context.Context, event E) error {
	select {
	case s.ch <- event:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *ChannelSink[E]) Outlet() <-chan E {
	return s.ch
}

type DrainSink[E any] struct{}

func (DrainSink[E]) Write(ctx context.Context, event E) error {
	return nil
}
