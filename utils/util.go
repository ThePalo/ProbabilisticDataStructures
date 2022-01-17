package utils

import (
	"encoding/csv"
	"fmt"
	"math"
	"math/bits"
	"math/rand"
	"os"

	"github.com/bits-and-blooms/bitset"
)

const Machine64Bits = 64
const ByteSize = 8

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
	//return CreateBigDataset()
	return ReadDatasetFromCsv()
}

func ReadDatasetFromCsv() ([][]byte, error) {
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

func ReadDatasetFromCsvAndFixLengthTo150k() ([][]byte, error) {
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
	for i := 0; i < 150000; i++ {
		usernames = append(usernames, []byte(csvLines[i][0]))
	}
	return usernames, nil
}

func ReadDatasetFromCsvAndFixLengthTo(size int) ([][]byte, error) {
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
	for i := 0; i < size; i++ {
		usernames = append(usernames, []byte(csvLines[i][0]))
	}
	return usernames, nil
}

func CreateBigDataset() ([][]byte, error) {
	size := 100000000
	dataset := make([][]byte, size)
	for i, _ := range dataset {
		dataset[i] = []byte(fmt.Sprint(i))
	}
	return dataset, nil
}

func CreateDataset() ([][]byte, error) {
	size := 300000
	dataset := make([][]byte, size)
	for i, _ := range dataset {
		dataset[i] = []byte(fmt.Sprint(i))
	}
	return dataset, nil
}

func CreateRandomDataset() ([][]byte, error) {
	size := 30000
	dataset := make([][]byte, size)
	for i, _ := range dataset {
		dataset[i] = []byte(fmt.Sprint(rand.Int()))
	}
	return dataset, nil
}