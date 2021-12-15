package utilities

import "math"

func NextPowerOf2(i uint) uint {
	if i < 2 {
		return 2
	}
	return uint(math.Pow(2, math.Ceil(math.Log2(float64(i)))))
}