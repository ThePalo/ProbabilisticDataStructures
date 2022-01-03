package cuckooFilter

import (
	"ProbabilisticDataStructures/utils"
	"encoding/binary"
	"math"
	"math/rand"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)

const (
	b          = 4
	defaultP   = uint(8)
	loadFactor = 0.95
	maxIterations = 500
	defaultSeed = 1564534
)

type CuckooFilter struct {
	n uint
	m uint
	p uint
	buckets []bucket
}

func New(m uint, p ...uint) CuckooFilter{
	n := computeCapacity(m)
	return newCF(n, m, p...)
}

func NewFromSize(n uint, p...uint) CuckooFilter{
	m := computeSizeM(n)
	return newCF(n, m, p...)
}

func (c *CuckooFilter) Insert(element []byte) bool {
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if c.insert(i, j, f) {
		return true
	}
	return false
}

func (c *CuckooFilter) InsertUnique(element []byte) bool {
	if c.Lookup(element) {
		return true
	}
	return c.Insert(element)
}

func (c *CuckooFilter) insert(i uint, j uint, f *bitset.BitSet) bool {
	if !c.buckets[i].isFull() {
		c.buckets[i].Add(f)
		return true
	}
	if !c.buckets[j].isFull() {
		c.buckets[j].Add(f)
		return true
	}
	k := utils.Sample(i, j)
	for n := 0; n < maxIterations; n++ {
		pos := uint(rand.Int() % b)
		f2 := c.buckets[k].Get(pos)
		c.buckets[k].AddInPosition(f, pos)
		k = c.getAlternativePosition(k, f2)
		if !c.buckets[k].isFull() {
			c.buckets[k].Add(f2)
			return true
		}
		f2.Copy(f)
	}
	return false
}

func (c *CuckooFilter) Lookup(element []byte) bool {
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if ok, _ := c.buckets[i].isElement(f); ok {
		return true
	}
	if ok, _ := c.buckets[j].isElement(f); ok {
		return true
	}
	return false
}

func (c *CuckooFilter) Delete(element []byte) bool {
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if ok, pos := c.buckets[i].isElement(f); ok {
		c.buckets[i].deletePos(pos)
		return true
	}
	if ok, pos := c.buckets[j].isElement(f); ok {
		c.buckets[j].deletePos(pos)
		return true
	}
	return false
}

func newCF (n uint, m uint, p ...uint) CuckooFilter{
	fingerprintSize := defaultP
	if len(p) > 0 {
		fingerprintSize = p[0]
	}
	return CuckooFilter{
		n: n,
		m: m,
		p: fingerprintSize,
		buckets: func (m uint) []bucket {
			l := make([]bucket, m)
			for i, _ := range l {
				l[i] = make(bucket, b)
			}
			return l
		}(m),
	}
}

func (c CuckooFilter) computeHashPositionsAndFingerprint(element []byte) (uint, uint, *bitset.BitSet) {
	hashed := computeHash(element, defaultSeed)
	i, f := c.getPositionAndFingerprint(hashed)
	j := c.getAlternativePosition(i, f)
	return i, j, f
}

func computeHash(element []byte, seed uint32) uint {
	if utils.RunningIn64BitMachine() {
		return uint(murmur3.Sum64WithSeed(element, seed))
	}
	return uint(murmur3.Sum32WithSeed(element, seed))
}

func (c CuckooFilter) getPositionAndFingerprint(hash uint) (uint, *bitset.BitSet) {
	bitHash := bitset.From([]uint64{uint64(hash)})
	f := bitset.New(c.p)
	for i := uint(0); i < c.p; i++ {
		f.SetTo(i, bitHash.Test(i))
	}
	i := (hash >> c.p) % c.m
	return i, f
}

func (c CuckooFilter) getAlternativePosition(i uint, f *bitset.BitSet) uint {
	bitHash := make([]byte, 8)
	binary.LittleEndian.PutUint64(bitHash, uint64(utils.BitSetToUint(f)))
	h := computeHash(bitHash, defaultSeed)
	setH := bitset.From([]uint64{uint64(h)})
	setI := bitset.From([]uint64{uint64(i)})
	position := bitset.New(64)

	for j := uint(0); j < position.Len(); j++ {
		bit := setH.Test(j) != setI.Test(j)
		position.SetTo(j, bit)
	}

	return utils.BitSetToUint(position) % c.m
}

func (c CuckooFilter) computeError() float64 {
	return 2*b/(math.Pow(2, float64(c.p)))
}

func computeSizeM(size uint) uint {
	return utils.NextPowerOf2(uint(math.Ceil(float64(size) / (loadFactor * b))))
}

func computeCapacity(m uint) uint {
	return uint(math.Floor(float64(m) * float64(b) * loadFactor))
}