package service

import (
	"ConcarenncyHits/internal/models/bid"
	"ConcarenncyHits/internal/models/semaphor"
	"ConcarenncyHits/internal/models/storage"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MinPriority = 1
	MaxPriority = 3
)

// WorkRequest — Go-аналог promise/future.
// GroupManager отправляет запрос диспетчеру и ждёт ответ в Reply.
// Dispatcher выбирает заявку и отправляет её в Reply.
type WorkRequest struct {
	GroupNumber int
	Reply       chan bid.Bid
}

type Config struct {
	StorageCapacity int
	GroupsCount     int
	DevicesPerGroup int
}

type System struct {
	cfg           Config
	storage       *storage.Storage
	incoming      chan bid.Bid
	workRequests  chan WorkRequest
	activeByGroup []int32
	generatedID   int32
}

func NewSystem(cfg Config) *System {
	return &System{
		cfg:           cfg,
		storage:       storage.CrtStorage(cfg.StorageCapacity),
		incoming:      make(chan bid.Bid),
		workRequests:  make(chan WorkRequest),
		activeByGroup: make([]int32, cfg.GroupsCount+1),
	}
}

func (s *System) Run(ctx context.Context, wg *sync.WaitGroup) {
	wg.Add(1)
	go s.Generator(ctx, wg)

	wg.Add(1)
	go s.QueueDispatcher(ctx, wg)

	for group := 1; group <= s.cfg.GroupsCount; group++ {
		wg.Add(1)
		go s.GroupManager(ctx, wg, group)
	}
}

func (s *System) Generator(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		delay := time.Duration(rand.Intn(2500)+500) * time.Millisecond

		select {
		case <-ctx.Done():
			fmt.Println("[generator] stopped")
			return
		case <-time.After(delay):
		}

		b := bid.Bid{
			ID:          int(atomic.AddInt32(&s.generatedID, 1)),
			GroupNumber: rand.Intn(s.cfg.GroupsCount) + 1,
			Priority:    rand.Intn(MaxPriority) + 1,
		}

		select {
		case s.incoming <- b:
			fmt.Printf("[generator] created %v\n", b)
		case <-ctx.Done():
			fmt.Println("[generator] stopped")
			return
		}
	}
}

func (s *System) QueueDispatcher(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	pending := make([][]chan bid.Bid, s.cfg.GroupsCount+1)
	statTicker := time.NewTicker(1 * time.Second)
	defer statTicker.Stop()

	for {
		// Если накопитель заполнен, диспетчер временно перестаёт читать incoming.
		// Из-за этого генератор блокируется на отправке и фактически "засыпает".
		var incoming <-chan bid.Bid
		if !s.storage.IsFull() {
			incoming = s.incoming
		}

		select {
		case b := <-incoming:
			if s.storage.Add(b) {
				fmt.Printf("[dispatcher] stored %v | storage=%d/%d\n", b, s.storage.Size(), s.storage.CapStorage)
			}

		case req := <-s.workRequests:
			selected, ok := s.storage.TakeBestForGroup(req.GroupNumber)
			if ok {
				fmt.Printf("[dispatcher] sent %v to group=%d | storage=%d/%d\n", selected, req.GroupNumber, s.storage.Size(), s.storage.CapStorage)
				s.safeReply(ctx, req.Reply, selected)
			} else {
				pending[req.GroupNumber] = append(pending[req.GroupNumber], req.Reply)
			}

		case <-statTicker.C:
			s.printDispatcherStats()

		case <-ctx.Done():
			fmt.Println("[dispatcher] stopped")
			return
		}

		s.flushPending(ctx, pending)
	}
}

func (s *System) flushPending(ctx context.Context, pending [][]chan bid.Bid) {
	for group := 1; group <= s.cfg.GroupsCount; group++ {
		for len(pending[group]) > 0 {
			selected, ok := s.storage.TakeBestForGroup(group)
			if !ok {
				break
			}

			reply := pending[group][0]
			pending[group] = pending[group][1:]

			fmt.Printf("[dispatcher] fulfilled pending request: %v to group=%d | storage=%d/%d\n", selected, group, s.storage.Size(), s.storage.CapStorage)
			s.safeReply(ctx, reply, selected)
		}
	}
}

func (s *System) safeReply(ctx context.Context, reply chan<- bid.Bid, b bid.Bid) {
	select {
	case reply <- b:
	case <-ctx.Done():
	}
}

func (s *System) printDispatcherStats() {
	fmt.Printf("[stats] central storage=%d/%d |", s.storage.Size(), s.storage.CapStorage)
	for group := 1; group <= s.cfg.GroupsCount; group++ {
		active := atomic.LoadInt32(&s.activeByGroup[group])
		fmt.Printf(" group-%d active=%d/%d;", group, active, s.cfg.DevicesPerGroup)
	}
	fmt.Println()
}

func (s *System) GroupManager(ctx context.Context, wg *sync.WaitGroup, groupNumber int) {
	defer wg.Done()

	limit := s.cfg.DevicesPerGroup * 80 / 100
	if limit < 1 {
		limit = 1
	}

	sem := semaphor.NewSemaphor(limit)
	jobs := make(chan bid.Bid)

	var devicesWG sync.WaitGroup
	for deviceID := 1; deviceID <= s.cfg.DevicesPerGroup; deviceID++ {
		devicesWG.Add(1)
		go s.Device(ctx, &devicesWG, groupNumber, deviceID, jobs, sem)
	}

	defer func() {
		close(jobs)
		devicesWG.Wait()
		fmt.Printf("[manager] group=%d stopped\n", groupNumber)
	}()

	for {
		if !sem.AcquireCtx(ctx) {
			return
		}

		reply := make(chan bid.Bid)

		select {
		case s.workRequests <- WorkRequest{GroupNumber: groupNumber, Reply: reply}:
		case <-ctx.Done():
			sem.Release()
			return
		}

		select {
		case b := <-reply:
			select {
			case jobs <- b:
			case <-ctx.Done():
				sem.Release()
				return
			}

		case <-ctx.Done():
			sem.Release()
			return
		}
	}
}

func (s *System) Device(ctx context.Context, wg *sync.WaitGroup, groupNumber int, deviceID int, jobs <-chan bid.Bid, sem *semaphor.Semaphor) {
	defer wg.Done()

	fmt.Printf("[device] group=%d device=%d state=idle\n", groupNumber, deviceID)

	for {
		select {
		case b, ok := <-jobs:
			if !ok {
				fmt.Printf("[device] group=%d device=%d stopped\n", groupNumber, deviceID)
				return
			}

			atomic.AddInt32(&s.activeByGroup[groupNumber], 1)
			workSeconds := rand.Intn(5) + 2

			for remaining := workSeconds; remaining > 0; remaining-- {
				fmt.Printf(
					"[device] group=%d device=%d state=busy bid=%d priority=%d remaining=%ds activeGroup=%d\n",
					groupNumber,
					deviceID,
					b.ID,
					b.Priority,
					remaining,
					atomic.LoadInt32(&s.activeByGroup[groupNumber]),
				)

				select {
				case <-time.After(1 * time.Second):
				case <-ctx.Done():
					atomic.AddInt32(&s.activeByGroup[groupNumber], -1)
					sem.Release()
					fmt.Printf("[device] group=%d device=%d stopped while busy\n", groupNumber, deviceID)
					return
				}
			}

			atomic.AddInt32(&s.activeByGroup[groupNumber], -1)
			sem.Release()
			fmt.Printf("[device] group=%d device=%d state=idle\n", groupNumber, deviceID)

		case <-ctx.Done():
			fmt.Printf("[device] group=%d device=%d stopped\n", groupNumber, deviceID)
			return
		}
	}
}
