package eventqueue

import "time"

type Option[E any] func(q *EventQueue[E])

// WithRetryInterval returns Option that sets the retry interval to q.
// Without this option, q does not retry to Write until another push event occurs.
func WithRetryInterval[E any](retryTimeout time.Duration) Option[E] {
	return func(q *EventQueue[E]) {
		q.retryTimeout = retryTimeout
	}
}

func WithReservationTimeout[E any](reservationTimeout time.Duration) Option[E] {
	return func(q *EventQueue[E]) {
		q.reservationTimeout = reservationTimeout
	}
}
