package eventqueue

import (
	"context"
	"sync"
	"testing"
)

type swappable[T any] struct {
	sync.Mutex
	t       *testing.T
	blocker chan struct{}
	sink    Sink[T]
}

func newSwappable[T any](t *testing.T) *swappable[T] {
	return &swappable[T]{
		t:       t,
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
	s.t.Logf("Write is called with %#v", e)
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
