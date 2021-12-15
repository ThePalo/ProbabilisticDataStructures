package bloomFilter

import (
	"math"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)



type BloomFilter struct {
	n uint
	m uint
	k uint
	e float64
	bits *bitset.BitSet
}

func New(size uint, error float64) BloomFilter {
	sizeM := computeSizeM(size, error)
	sizeK := computeSizeK(size, sizeM)

	return BloomFilter{
		n: size,
		e: error,
		m: sizeM,
		k: sizeK,
		bits: bitset.New(sizeM),
	}
}

func (b *BloomFilter) Insert(element []byte) {
	positions, err := b.computeHashPositions(element)
	if err != nil {
		panic(err)
	}
	for _, pos := range positions {
		b.bits.Set(pos)
	}
}

func (b BloomFilter) Lookup(element []byte) bool {
	positions, err := b.computeHashPositions(element)
	if err != nil {
		panic(err)
	}
	for _, pos := range positions {
		if !b.bits.Test(pos) {
			return false
		}
	}
	return true
}

func computeSizeM(n uint, error float64) uint {
	return uint(math.Ceil(-(float64(n)*math.Log(error))/(math.Log(2)*math.Log(2))))
}

func computeSizeK(n uint, sizeM uint) uint {
	return uint(math.Ceil((float64(sizeM)/float64(n))*math.Log(2)))
}

func (b BloomFilter) computeHashPositions(element []byte) ([]uint, error) {
	positions := make([]uint, 0)
	for i := uint(0); i < b.k; i++ {
		pos := uint(murmur3.Sum64WithSeed(element, uint32(i)))%b.m
		positions = append(positions, pos)
	}
	return positions, nil
}