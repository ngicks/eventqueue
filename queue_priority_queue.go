package eventqueue

import (
	"slices"

	"github.com/ngicks/go-common/generic/priorityqueue"
)

var _ Queue[any] = (*PriorityQueue[any])(nil)

// PriorityQueue implements [Queue].
type PriorityQueue[T any] struct {
	popped []T
	q      *priorityqueue.Filterable[T]
}

func NewPriorityQueue[T any](init []T, less func(i T, j T) bool, methods priorityqueue.SliceInterfaceMethods[T]) *PriorityQueue[T] {
	return &PriorityQueue[T]{
		q: priorityqueue.NewFilterable(init, less, methods),
	}
}

func (q *PriorityQueue[T]) Range(fn func(i int, e T) (next bool)) {
	for i, e := range q.popped {
		if !fn(i, e) {
			return
		}
	}
	c := q.q.Clone()
	for i := len(q.popped); c.Len() > 0; i++ {
		p := c.Pop()
		if !fn(i, p) {
			return
		}
	}
}

func (q *PriorityQueue[T]) Clone() []T {
	s := make([]T, len(q.popped), q.Len())
	_ = copy(s, q.popped)
	c := q.q.Clone()
	for c.Len() > 0 {
		s = append(s, c.Pop())
	}
	return s
}

func (q *PriorityQueue[T]) Clear() {
	q.popped = slices.Delete(q.popped, 0, len(q.popped))
	q.q.Filter(func(s []T) []T {
		return slices.Delete(s, 0, len(s))
	})
}
func (q *PriorityQueue[T]) Len() int {
	return len(q.popped) + q.q.Len()
}
func (q *PriorityQueue[T]) PopFront() T {
	if len(q.popped) > 0 {
		last := len(q.popped) - 1
		popped := q.popped[last]
		var zero T
		q.popped[last] = zero
		q.popped = q.popped[:last]
		return popped
	}
	return q.q.Pop()
}
func (q *PriorityQueue[T]) PushBack(elem T) {
	q.q.Push(elem)
}
func (q *PriorityQueue[T]) PushFront(elem T) {
	q.popped = append(q.popped, elem)
}
