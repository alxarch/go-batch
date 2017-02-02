package batch_test

import (
	"context"
	"testing"
	"time"

	"github.com/alxarch/go-batch"
)

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
	ctx, cancel := context.WithCancel(context.Background())
	c := q.Run(ctx)
	if ctx != c {
		t.Error("Not same c")
	}
	q.Add("a")
	q.Add("b")
	q.Add("c")
	b := <-done
	if len(b) != 2 {
		t.Error("Invalid batch length")
	}
	if v, ok := b[0].(string); !ok || v != "a" {
		t.Error("Invalid batch elem 0")
	}
	if v, ok := b[1].(string); !ok || v != "b" {
		t.Error("Invalid batch elem 1 %s", v)
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
