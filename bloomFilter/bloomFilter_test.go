package bloomFilter

import (
	"fmt"
	"math"
	"testing"
)

const (
	roundTo                  = 100000
	errorRangeFalsePositives = 0.15
)

func TestNew(t *testing.T) {
	bFList := []BloomFilter{
		{
			n: 84,
			m: 730,
			k: 6,
			e: 0.015370,
		},
	}
	for _, expectedBF := range bFList {
		bf := New(expectedBF.m, expectedBF.k)
		if bf.n != expectedBF.n {
			t.Errorf("Expected capacity %d, Current capacity %d", expectedBF.n, bf.n)
		}
		if math.Round(bf.e*roundTo)/roundTo != math.Round(expectedBF.e*roundTo)/roundTo {
			t.Errorf("Expected error %.5f, Current error %.5f", expectedBF.e, bf.e)
		}
		if bf.m != expectedBF.m {
			t.Errorf("Expected size %d, Current size %d", expectedBF.m, bf.m)
		}
		if bf.k != expectedBF.k {
			t.Errorf("Expected # hash functions %d, Current # hash functions %d", expectedBF.k, bf.k)
		}
	}
}

func TestNewFromSizeAndError(t *testing.T) {
	bFList := []BloomFilter{
		{
			n: 100,
			m: 730,
			k: 6,
			e: 0.03,
		},
	}
	for _, expectedBF := range bFList {
		bf := NewFromSizeAndError(expectedBF.n, expectedBF.e)
		if bf.n != expectedBF.n {
			t.Errorf("Expected capacity %d, Current capacity %d", expectedBF.n, bf.n)
		}
		if math.Round(bf.e*roundTo)/roundTo != math.Round(expectedBF.e*roundTo)/roundTo {
			t.Errorf("Expected error %.5f, Current error %.5f", expectedBF.e, bf.e)
		}
		if bf.m != expectedBF.m {
			t.Errorf("Expected size %d, Current size %d", expectedBF.m, bf.m)
		}
		if bf.k != expectedBF.k {
			t.Errorf("Expected # hash functions %d, Current # hash functions %d", expectedBF.k, bf.k)
		}
	}
}

func TestInsertAndLookup(t *testing.T) {
	b := NewFromSizeAndError(100, 0.03)
	elem1 := []byte("Hello World")
	elem2 := []byte("A")
	elem3 := []byte("Bye")
	b.Insert(elem1)
	b.Insert(elem2)
	if ok := b.Lookup(elem1); !ok {
		t.Errorf("%s should be in.", elem1)
	}
	if ok := b.Lookup(elem2); !ok {
		t.Errorf("%s should be in.", elem2)
	}
	if ok := b.Lookup(elem3); ok {
		t.Errorf("%s should NOT be in.", elem3)
	}
}

func TestInsertAndLookupWithHighLoadFactor(t *testing.T) {
	size := uint(100)
	e := 0.003
	b := NewFromSizeAndError(size, e)
	listElement := make([]string, size)
	for i := uint(0); i < size; i++ {
		listElement[i] = fmt.Sprintf("%d", i)
		b.Insert([]byte(listElement[i]))
	}
	// Look up for elements already in the filter
	for _, elem := range listElement {
		if ok := b.Lookup([]byte(elem)); !ok {
			t.Errorf("%s should be in.", elem)
		}
	}
	// Look up for elements that NOT exists in the filter
	elementsToTest := 1000000
	falsePositives := 0
	for i := uint(0); i < uint(elementsToTest); i++ {
		toInsert := i + size + 1
		elem := fmt.Sprintf("%d", toInsert)
		if ok := b.Lookup([]byte(elem)); ok {
			falsePositives++
		}
	}
	expectedFalsePositives := int(float64(elementsToTest) * e)
	rangeFalsePositives := int(math.Ceil(float64(expectedFalsePositives) * errorRangeFalsePositives))
	currentRange := falsePositives - expectedFalsePositives
	if currentRange > 0 && currentRange > rangeFalsePositives {
		t.Errorf("Error: Expected false positives are %d ± %d and current false positives are %d", expectedFalsePositives, rangeFalsePositives, falsePositives)
	}
}
