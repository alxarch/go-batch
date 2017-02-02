package batch

import (
	"context"
	"time"
)

type Drainer interface {
	Drain(context.Context, []interface{}) error
}

type DrainFunc func(context.Context, []interface{}) error

func (df DrainFunc) Drain(ctx context.Context, batch []interface{}) error {
	return df(ctx, batch)
}

type Queue struct {
	Interval time.Duration
	Size     int
	Drainer  Drainer

	queue chan interface{}
}

const DefaultSize = 1000
const DefaultInterval = time.Second

func (s *Queue) Run(ctx context.Context) context.Context {
	if nil == ctx {
		ctx = context.Background()
	}
	size := s.Size
	if size <= 0 {
		size = DefaultSize
	}
	s.queue = make(chan interface{}, 2*size)
	if s.Interval <= 0 {
		s.Interval = DefaultInterval
	}
	tick := time.NewTicker(s.Interval)
	batch := make([]interface{}, 0, size)
	drain := func() {
		if len(batch) > 0 {
			// Non-blocking drain
			if s.Drainer != nil {
				go func(b []interface{}) {
					s.Drainer.Drain(ctx, b)
				}(batch)
			}
			batch = make([]interface{}, 0, size)
		}
	}

	go func() {
		defer close(s.queue)
		defer tick.Stop()
		defer drain()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				drain()
			case x := <-s.queue:
				if len(batch) == cap(batch) {
					drain()
				}
				batch = append(batch, x)
			}
		}
	}()
	return ctx
}

func (s *Queue) Add(x interface{}) {
	if nil == s.queue {
		s.Run(nil)
	}

	s.queue <- x
}
