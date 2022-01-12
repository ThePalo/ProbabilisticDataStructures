package utils

import (
	"encoding/csv"
	"math"
	"math/bits"
	"math/rand"
	"os"

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

func ReadDataset() ([][]byte, error) {
	csvFile, err := os.Open("../pyTwitterApi/dataset.csv")
	if err != nil {
		return [][]byte{}, err
	}
	defer csvFile.Close()
	csvLines, err := csv.NewReader(csvFile).ReadAll()
	if err != nil {
		return [][]byte{}, err
	}
	var usernames [][]byte
	for _, line := range csvLines {
		usernames = append(usernames, []byte(line[0]))
	}
	return usernames, nil
}