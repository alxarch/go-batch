package batch_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alxarch/go-batch"
)

func Test_Retries(t *testing.T) {
	n := int64(0)
	done := make(chan struct{})
	q := &batch.Queue{
		Size:     2,
		Retries:  2,
		Interval: 5 * time.Second,
		Drainer: batch.DrainFunc(func(ctx context.Context, b []interface{}) error {
			if atomic.AddInt64(&n, 1) > 2 {
				close(done)
				return nil
			}
			return errors.New("ERR")
		}),
	}
	q.MustRun(nil)
	q.Add(1)
	q.Add(1)
	<-done
	if n != 3 {
		t.Error("No retries")
	}
}
func Test_Queue(t *testing.T) {

	done := make(chan []interface{}, 1)
	q := &batch.Queue{
		Size:     2,
		Interval: 5 * time.Second,
		Drainer: batch.DrainFunc(func(ctx context.Context, b []interface{}) error {
			done <- b
			return nil
		}),
	}
	err := q.Add("foo")
	if err != batch.NotRunningError {
		t.Error("Invalid not running error")
	}
	ctx, cancel := context.WithCancel(context.Background())
	q.MustRun(ctx)
	if err := q.Run(nil); err != batch.AlreadyRunningError {
		t.Error("Invalid running error")

	}

	q.MustAdd("a")
	q.Add("b")
	q.Add("c")
	b := <-done
	if len(b) != 2 {
		t.Error("Invalid batch length %d", len(b))
	}
	if v, ok := b[0].(string); !ok || v != "a" {
		t.Error("Invalid batch elem 0")
	}
	if v, ok := b[1].(string); !ok || v != "b" {
		t.Error("Invalid batch elem 1 %s", v)
	}
	items, fsize, ftick, ferr := q.Stats()
	if items != 3 {
		t.Error("Invalid stats items %d", items)
	}
	if fsize != 1 {
		t.Error("Invalid stats fsize")
	}
	if ftick != 0 {
		t.Error("Invalid stats ftick")
	}
	if ferr != 0 {
		t.Error("Invalid stats ferr")
	}

	cancel()

	b = <-done
	if len(b) != 1 {
		t.Error("Invalid batch length")
	}
	if v, ok := b[0].(string); !ok || v != "c" {
		t.Error("Invalid batch elem 0")
	}
}
