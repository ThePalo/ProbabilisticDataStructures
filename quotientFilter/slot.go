package quotientFilter

import (
	"github.com/bits-and-blooms/bitset"
)

const defaultSize = uint(10)

type slot struct {
	isOccupied     bool
	isContinuation bool
	isShifted      bool
	reminder       *bitset.BitSet
}

func (s slot) isEmpty() bool {
	return s.reminder == nil
}

func (s *slot) add(fq *bitset.BitSet) {
	size := defaultSize
	if fq != nil {
		size = fq.Len()
	}
	if s.reminder == nil {
		s.reminder = bitset.New(size)
	}
	if fq != nil {
		fq.Copy(s.reminder)
		return
	}
	s.reminder = nil
}

func (s slot) getSlot() slot {
	s1 := slot{
		isOccupied:     s.isOccupied,
		isContinuation: s.isContinuation,
		isShifted:      s.isShifted,
	}
	s1.add(s.reminder)
	return s1
}

func (s slot) getReminder() *bitset.BitSet {
	return s.reminder
}

func (s *slot) delete() {
	s.reminder = nil
}

func (s slot) isInitCluster() bool {
	return s.isOccupied && !s.isContinuation && !s.isShifted
}

func (s slot) isInitRun() bool {
	return s.isInitCluster() || (!s.isContinuation && s.isShifted)
}
