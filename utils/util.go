package utils

import (
	"math"
	"math/bits"
	"math/rand"

	"github.com/bits-and-blooms/bitset"
)

const Machine64Bits = 64

func NextPowerOf2(i uint) uint {
	if i < 2 {
		return 2
	}
	return uint(math.Pow(2, math.Ceil(math.Log2(float64(i)))))
}

func BitSetToUint(set *bitset.BitSet) uint {
	return uint(set.Bytes()[0])
}

func RunningIn64BitMachine() bool {
	return bits.UintSize == Machine64Bits
}

func Sample(i uint, j uint) uint {
	if rand.Int()%100 < 50 {
		return i
	}
	return j
}
