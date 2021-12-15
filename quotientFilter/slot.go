package quotientFilter

import (
	"github.com/bits-and-blooms/bitset"
)

const (
	metadataSize = 3
	isOccupied = 0
	isContinuation = 1
	isShifted = 2
)

type slot struct {
	metadata *bitset.BitSet
	reminder *bitset.BitSet
}

func (s slot) isShifted() bool {
	return s.metadata.Test(isShifted)
}

func (s slot) isOccupied() bool {
	return s.metadata.Test(isOccupied)
}

func (s slot) isContinuation() bool {
	return s.metadata.Test(isContinuation)
}

func (s *slot) setShifted() {
	s.metadata = s.metadata.Set(isShifted)
}

func (s *slot) setOccupied() {
	s.metadata = s.metadata.Set(isOccupied)
}

func (s *slot) setContinuation() {
	s.metadata = s.metadata.Set(isContinuation)
}

func (s *slot) setTo (bit uint, val bool) {
	s.metadata = s.metadata.SetTo(bit, val)
}

func (s slot) isEmpty() bool {
	return s.reminder == nil
}

func (s *slot) add(fq *bitset.BitSet, size uint) {
	if s.reminder == nil {
		s.reminder = bitset.New(8)
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