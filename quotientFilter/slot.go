package quotientFilter

import (
	"github.com/bits-and-blooms/bitset"
)

type slot struct {
	isOccupied bool
	isContinuation bool
	isShifted bool
	reminder *bitset.BitSet
}


func (s slot) isEmpty() bool {
	return s.reminder == nil
}

func (s *slot) add(fq *bitset.BitSet, size uint) {
	if s.reminder == nil {
		s.reminder = bitset.New(size)
	}
	if fq != nil {
		fq.Copy(s.reminder)
		return
	}
	s.reminder = nil
}

func (s slot) get() *bitset.BitSet {
	return s.reminder
}

func (s *slot) delete()  {
	s.reminder = nil
}