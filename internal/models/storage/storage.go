package storage

import "ConcarenncyHits/internal/models/bid"

type Storage struct {
	CapStorage int
	Bids       []bid.Bid
}

func CrtStorage(capStorage int) *Storage {
	return &Storage{
		CapStorage: capStorage,
		Bids:       make([]bid.Bid, 0, capStorage),
	}
}

func (s *Storage) IsFull() bool {
	return len(s.Bids) >= s.CapStorage
}

func (s *Storage) Size() int {
	return len(s.Bids)
}

func (s *Storage) Add(b bid.Bid) bool {
	if s.IsFull() {
		return false
	}

	s.Bids = append(s.Bids, b)
	return true
}

func (s *Storage) TakeBestForGroup(groupNumber int) (bid.Bid, bool) {
	bestIndex := -1
	bestPriority := -1

	for i, b := range s.Bids {
		if b.GroupNumber != groupNumber {
			continue
		}

		if b.Priority > bestPriority {
			bestIndex = i
			bestPriority = b.Priority
		}
	}

	if bestIndex == -1 {
		return bid.Bid{}, false
	}

	selected := s.Bids[bestIndex]
	s.Bids = append(s.Bids[:bestIndex], s.Bids[bestIndex+1:]...)
	return selected, true
}
