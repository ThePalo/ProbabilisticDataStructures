package cuckooFilter

import "github.com/bits-and-blooms/bitset"

type bucket []*bitset.BitSet

func (b *bucket) deletePos(pos uint) {
	(*b)[pos] = nil
}

func (b bucket) isElement(f *bitset.BitSet) (bool, uint) {
	pos := uint(0)
	for _, elem := range b {
		if elem != nil && elem.Equal(f) {
			return true, pos
		}
		pos++
	}
	return false, 0
}

func (b bucket) isFull() bool {
	for _, elem := range b {
		if elem == nil {
			return false
		}
	}
	return true
}

func (b *bucket) Add(f *bitset.BitSet) {
	if f == nil {
		return
	}
	for i, _ := range *b {
		if (*b)[i] == nil {
			(*b)[i] = bitset.New(f.Len())
			f.Copy((*b)[i])
			return
		}
	}
}

func (b *bucket) AddInPosition(f *bitset.BitSet, pos uint) {
	f.Copy((*b)[pos])
}

func (b bucket) Get(pos uint) *bitset.BitSet {
	n := bitset.New(b[pos].Len())
	b[pos].Copy(n)
	return n
}
