package eventqueue

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ngicks/gommon/pkg/timing"
	"github.com/stretchr/testify/assert"
)

func TestChannelSink(t *testing.T) {
	assert := assert.New(t)

	sink := NewChannelSink[int](0)

	var err atomic.Pointer[error]
	waiter := timing.CreateWaiterCh(func() {
		err_ := sink.Write(context.Background(), 1)
		err.Store(&err_)
	})

	select {
	case <-waiter:
		t.Error("Write must not be blocked without receiving the Outlet channel or cancelling the context.")
	case <-time.After(time.Microsecond):
	}

	assert.Equal(1, <-sink.Outlet())
	<-waiter
	assert.NoError(*err.Load())

	ctx, cancel := context.WithCancel(context.Background())

	waiter = timing.CreateWaiterCh(func() {
		err_ := sink.Write(ctx, 2)
		err.Store(&err_)
	})

	select {
	case <-waiter:
		t.Error("Write must not be blocked without receiving the Outlet channel or cancelling the context.")
	case <-time.After(time.Microsecond):
	}

	cancel()
	<-waiter
	assert.ErrorIs(*err.Load(), context.Canceled)
}
