package batch

import (
	"context"
	"errors"
	"sync/atomic"
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
	Size     int
	Interval time.Duration
	Drainer  Drainer
	Retries  int

	queue chan interface{}
	stats struct {
		items      uint64
		flush_size uint64
		flush_tick uint64
		flush_err  uint64
	}
	up int64
}

func (s *Queue) IsUp() bool {
	return atomic.LoadInt64(&s.up) > 0
}

func (s *Queue) Stats() (total, flush_size, flush_tick, flush_err uint64) {
	return atomic.LoadUint64(&s.stats.items),
		atomic.LoadUint64(&s.stats.flush_size),
		atomic.LoadUint64(&s.stats.flush_tick),
		atomic.LoadUint64(&s.stats.flush_err)
}

func (s *Queue) MustRun(ctx context.Context) {
	if err := s.Run(ctx); err != nil {
		panic(err)
	}
}

var (
	NoDrainerError = errors.New("No Drainer in Queue")
)

func (s *Queue) Flush(ctx context.Context, batch []interface{}) (err error) {
	if nil == s.Drainer {
		return NoDrainerError
	}
	r := 0
	if s.Retries > 0 {
		r = s.Retries
	}
	for ; r >= 0; r-- {
		if err = s.Drainer.Drain(ctx, batch); err == nil {
			break
		} else {
			atomic.AddUint64(&s.stats.flush_err, 1)
		}
	}
	return
}

var (
	AlreadyRunningError = errors.New("Queue already running.")
)

const (
	DefaultInterval = time.Second
	DefaultSize     = 1000
)

func (s *Queue) Run(ctx context.Context) error {
	if n := atomic.AddInt64(&s.up, 1); n > 1 {
		return AlreadyRunningError
	}
	interval := DefaultInterval
	if s.Interval >= 0 {
		interval = s.Interval
	}
	size := DefaultSize
	if s.Size > 0 {
		size = s.Size
	}
	tick := time.NewTicker(interval)
	if nil == ctx {
		ctx = context.Background()
	}

	s.queue = make(chan interface{}, 2*size)
	errors := make(chan error, 1)
	batch := make([]interface{}, 0, size)
	flush := func() {
		// Non-blocking drain
		go func(batch []interface{}) {
			if err := s.Flush(ctx, batch); err != nil {
				errors <- err
			}
		}(batch)
		batch = make([]interface{}, 0, s.Size)
	}

	go func() {
		// Drain last batch if needed
		defer func() {
			s.up = 0
			close(s.queue)
			close(errors)
			tick.Stop()
			if len(batch) > 0 {
				flush()
			}
		}()
		for {
			select {
			case <-ctx.Done():
				return
			case _ = <-errors:
				s.stats.flush_err++
			case <-tick.C:
				if len(batch) > 0 {
					flush()
					s.stats.flush_tick++
				}
			case x := <-s.queue:
				if len(batch) == cap(batch) {
					flush()
					s.stats.flush_size++
				}
				batch = append(batch, x)
				s.stats.items++
			}
		}

	}()

	return nil
}

var (
	NotRunningError = errors.New("Queue is not running")
)

func (s *Queue) MustAdd(x interface{}) {
	if err := s.Add(x); err != nil {
		panic(err)
	}
}

func (s *Queue) Add(x interface{}) error {
	if nil == s.queue {
		return NotRunningError
	}
	s.queue <- x
	return nil
}
