package eventqueue

import "time"

type Option[E any] func(q *EventQueue[E])

// WithRetryInterval returns Option that sets the retry interval to q.
// Without this option, q does not retry to Write until another push event occurs.
//
// The duration less than or equals to 0 disables retry.
//
// The default is 0.
func WithRetryInterval[E any](retryTimeout time.Duration) Option[E] {
	return func(q *EventQueue[E]) {
		q.retryTimeout = retryTimeout
	}
}

// WithReservationTimeout sets timeout duration for tasks reserved by Reserve method.
// The context.Context passed to fn will be canceled after this duration has passed.
//
// The duration less than or equals to 0 disables timeout.
//
// The default is 0.
func WithReservationTimeout[E any](reservationTimeout time.Duration) Option[E] {
	return func(q *EventQueue[E]) {
		q.reservationTimeout = reservationTimeout
	}
}

// WithQueue option swaps an internal queueing mechanism to an arbitrary implementation.
//
// Setting nil falls back to the default.
//
// The default is Deque[T].
func WithQueue[E any](queue Queue[E]) Option[E] {
	return func(q *EventQueue[E]) {
		q.queue = queue
	}
}

// WithQueueSize sets soft limit on queue size.
//
// If queueSize is greater than 0,
// Push method and sending on Pusher channel blocks after the queue size exceeds this limit.
// The queueSize which is less than or equals to 0 places no limit on queued element size.
//
// This does not place a strict limit;
// the limit is totally ignored by 2 ways. Pushes after reserved task completion and Pushing back an element failed by a Write error.
// The Pusher channel may take an additional single element than limit.
//
// **Caution**: When Write to Sink returns error at queue limit it may stuck forever at that state.
// You'll need to set WithRetryInterval along side for periodical retry,
// or customize Sink implementation to detect errors and stop queue at the occurrence.
func WithQueueSize[E any](queueSize int) Option[E] {
	return func(q *EventQueue[E]) {
		q.queueSize = queueSize
	}
}
