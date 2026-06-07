package semaphor

import "context"

type Semaphor struct {
	ch chan struct{}
}

func NewSemaphor(limit int) *Semaphor {
	if limit < 1 {
		limit = 1
	}

	return &Semaphor{
		ch: make(chan struct{}, limit),
	}
}

func (s *Semaphor) Acquire() {
	s.ch <- struct{}{}
}

func (s *Semaphor) AcquireCtx(ctx context.Context) bool {
	select {
	case s.ch <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func (s *Semaphor) Release() {
	<-s.ch
}
