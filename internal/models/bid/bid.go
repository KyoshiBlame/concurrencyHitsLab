package bid

import "fmt"

type Bid struct {
	ID          int
	GroupNumber int
	Priority    int
}

func (b Bid) String() string {
	return fmt.Sprintf("Bid{ID:%d Group:%d Priority:%d}", b.ID, b.GroupNumber, b.Priority)
}
