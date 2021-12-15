package utils

import (
	"math"

	"github.com/bits-and-blooms/bitset"
)

func NextPowerOf2(i uint) uint {
	if i < 2 {
		return 2
	}
	return uint(math.Pow(2, math.Ceil(math.Log2(float64(i)))))
}

func BitSetToUint(set *bitset.BitSet) uint {
	return uint(set.Bytes()[0])
}

func MaskLessImportantBits(num uint) uint {
	return (1 << num) - 1
}