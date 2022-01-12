package bloomFilter

import (
	"math"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)

// BloomFilter is the struct that represents a Bloom Filter
type BloomFilter struct {
	n     uint
	m     uint
	k     uint
	e     float64
	count uint
	bits *bitset.BitSet
}

// New creates a new Bloom Filter with size m and k hash functions
func New(m uint, k uint) BloomFilter {
	n := computeCapacity(m, k)
	e := computeError(m, k, n)
	return newBF(n, e, m, k)
}

// NewFromSizeAndError creates a new Bloom Filter that can hold n elements with e false positive error
func NewFromSizeAndError(n uint, e float64) BloomFilter {
	sizeM := computeSizeM(n, e)
	sizeK := computeSizeK(n, sizeM)
	return newBF(n, e, sizeM, sizeK)
}

// Insert inserts element into BF. Computational time: O(k)
func (b *BloomFilter) Insert(element []byte) {
	positions := b.computeKHashPositions(element)
	for _, pos := range positions {
		b.bits.Set(pos)
	}
	b.count++
}

// Lookup returns true if element may belong to the BF and false if element does not belong to the BF. Computational time: O(k)
func (b BloomFilter) Lookup(element []byte) bool {
	positions := b.computeKHashPositions(element)
	for _, pos := range positions {
		if !b.bits.Test(pos) {
			return false
		}
	}
	return true
}

func newBF(n uint, e float64, m uint, k uint) BloomFilter {
	return BloomFilter{
		n:    n,
		e:    e,
		m:    m,
		k:    k,
		count: 0,
		bits: bitset.New(m),
	}
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
	return uint(murmur3.Sum64WithSeed(element, seed))
}

func computeCapacity(m uint, k uint) uint {
	return uint(math.Floor(float64(m) / (float64(k)) * math.Log(2)))
}

func computeError(m uint, k uint, n uint) float64 {
	return math.Pow(1-math.Pow(math.E, -float64(k)*float64(n)/float64(m)), float64(k))
}

func computeSizeM(n uint, error float64) uint {
	return uint(math.Ceil(-(float64(n) * math.Log(error)) / (math.Log(2) * math.Log(2))))
}

func computeSizeK(n uint, sizeM uint) uint {
	return uint(math.Ceil((float64(sizeM) / float64(n)) * math.Log(2)))
}
