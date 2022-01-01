package cuckooFilter

import (
	"ProbabilisticDataStructures/utils"
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"

	"github.com/spaolacci/murmur3"
)

const (
	bucketSize = 4
	fingerprintSize = 8
	loadFactor = 0.95
	maxIterations = 500
)

type bucket []byte

type CuckooFilter struct {
	n uint
	m uint
	buckets []bucket
}

func New(size uint) CuckooFilter {
	sizeM := computeSizeM(size)
	fmt.Println(sizeM)
	return CuckooFilter{
		n: size,
		m: sizeM,
		buckets: make([]bucket, sizeM),
	}
}

func (c *CuckooFilter) Insert(element []byte) bool{
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if len(c.buckets[i]) < bucketSize {
		c.buckets[i] = append(c.buckets[i], f)
		return true
	}
	if len(c.buckets[j]) < bucketSize {
		c.buckets[j] = append(c.buckets[j], f)
		return true
	}
	k := sample(i, j)
	for n := 0; n < maxIterations; n++ {
		pos := rand.Int()%bucketSize
		f2 := c.buckets[k][pos]
		c.buckets[k][pos] = f
		k = c.getAlternativePosition(k, f2)
		if len(c.buckets[k]) < bucketSize {
			c.buckets[k] = append(c.buckets[k], f2)
			return true
		}
		f = f2
	}
	return false
}

func (c *CuckooFilter) Lookup(element []byte) bool {
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if ok, _ := isElement(c.buckets[i], f); ok {
		return true
	}
	if ok, _ := isElement(c.buckets[j], f); ok {
		return true
	}
	return false
}

func (c *CuckooFilter) Delete(element []byte) bool {
	i, j, f := c.computeHashPositionsAndFingerprint(element)
	if ok, pos := isElement(c.buckets[i], f); ok {
		c.buckets[i] = deletePos(c.buckets[i], pos)
		return true
	}
	if ok, pos := isElement(c.buckets[j], f); ok {
		c.buckets[j] = deletePos(c.buckets[j], pos)
		return true
	}
	return false
}

func deletePos(b bucket, pos uint) bucket {
	newBucket := make(bucket, 0)
	for i, elem := range b {
		if uint(i) != pos {
			newBucket = append(newBucket, elem)
		}
	}
	return newBucket
}

func computeSizeM(size uint) uint {
	return utils.NextPowerOf2(uint(math.Ceil(float64(size) / (loadFactor * bucketSize))))
}

func (c CuckooFilter) computeHashPositionsAndFingerprint(element []byte) (uint, uint, byte) {
	hash := murmur3.Sum64(element)
	f := getLessImportantByteFromHash(hash)
	i := uint(hash>>32)%c.m
	j := c.getAlternativePosition(i, f)
	return i, j, f
}

func (c CuckooFilter) getAlternativePosition(i uint, f byte) uint {
	h := murmur3.Sum64([]byte{f})
	hf := getLessImportantByteFromHash(h)
	return uint(byte(i & 0x00FF) ^ hf) % c.m
}

func getLessImportantByteFromHash(hash uint64) byte {
	return byte(hash & 0x00FF)
}

func (c *CuckooFilter) hashes(data string) (uint, uint, []byte) {
	h := hash([]byte(data))
	f := h[0:fingerprintSize]
	i1 := uint(binary.BigEndian.Uint32(h))
	i2 := i1 ^ uint(binary.BigEndian.Uint32(hash(f)))
	return i1, i2, f
}

func hash(data []byte) []byte {
	hasher := sha1.New()
	hasher.Write([]byte(data))
	hash := hasher.Sum(nil)
	hasher.Reset()
	return hash
}

func isElement(b bucket, f byte) (bool, uint) {
	pos := uint(0)
	for _, elem := range b {
		if elem == f {
			return true, pos
		}
		pos++
	}
	return false, 0
}

func sample(i uint, j uint) uint {
	if rand.Int()%100 < 50 {
		return i
	}
	return j
}