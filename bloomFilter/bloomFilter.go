package bloomFilter

import (
	"math"
	"math/bits"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)

const machine64Bits = 64

type BloomFilter struct {
	n uint
	m uint
	k uint
	e float64
	bits *bitset.BitSet
}

func newBF(n uint, e float64, m uint, k uint) BloomFilter {
	return BloomFilter{
		n: n,
		e: e,
		m: m,
		k: k,
		bits: bitset.New(m),
	}
}

// New creates a new Bloom Filter
func New(m uint, k uint) BloomFilter {
	n := computeCapacity(m, k)
	e := computeError(m, k, n)
	return newBF(n, e, m, k)
}

func NewFromSizeAndError(size uint, error float64) BloomFilter {
	sizeM := computeSizeM(size, error)
	sizeK := computeSizeK(size, sizeM)
	return newBF(size, error, sizeM, sizeK)
}

func (b *BloomFilter) Insert(element []byte) {
	positions := b.computeKHashPositions(element)
	for _, pos := range positions {
		b.bits.Set(pos)
	}
}

func (b BloomFilter) Lookup(element []byte) bool {
	positions := b.computeKHashPositions(element)
	for _, pos := range positions {
		if !b.bits.Test(pos) {
			return false
		}
	}
	return true
}

func (b BloomFilter) computeKHashPositions(element []byte) []uint {
	positions := make([]uint, 0)
	for i := uint(0); i < b.k; i++ {
		pos := computeHash(element, uint32(i)) % b.m
		positions = append(positions, pos)
	}
	return positions
}

func computeHash(element []byte, seed uint32) uint {
	if bits.UintSize == machine64Bits {
		return uint(murmur3.Sum64WithSeed(element, seed))
	}
	return uint(murmur3.Sum32WithSeed(element, seed))
}

func computeCapacity(m uint, k uint) uint {
	return uint(math.Floor(float64(m)/(float64(k))*math.Log(2)))
}

func computeError(m uint, k uint, n uint) float64 {
	return math.Pow(1-math.Pow(math.E, -float64(k)*float64(n)/float64(m)), float64(k))
}

func computeSizeM(n uint, error float64) uint {
	return uint(math.Ceil(-(float64(n)*math.Log(error))/(math.Log(2)*math.Log(2))))
}

func computeSizeK(n uint, sizeM uint) uint {
	return uint(math.Ceil((float64(sizeM)/float64(n))*math.Log(2)))
}