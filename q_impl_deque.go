package eventqueue

import "github.com/gammazero/deque"

var _ Queue[any] = (*Deque[any])(nil)

type Deque[T any] struct {
	*deque.Deque[T]
}

func NewDeque[T any](cap int) *Deque[T] {
	return &Deque[T]{
		Deque: deque.New[T](cap),
	}
}

func (q *Deque[T]) Range(fn func(i int, e T) (next bool)) {
	for i := 0; i < q.Len(); i++ {
		if !fn(i, q.At(i)) {
			break
		}
	}
}

func (q *Deque[T]) Clone() []T {
	cloned := make([]T, q.Len())
	for i := 0; i < q.Len(); i++ {
		cloned[i] = q.At(i)
	}
	return cloned
}
