package cuckooFilter

import (
	"ProbabilisticDataStructures/utils"
	"fmt"
	"math"
	"testing"
)

const errorRangeFalsePositives = 0.1

func TestComputeHashPositionsAndFingerprint(t *testing.T) {
	for p := uint(4); p <= utils.Machine64Bits; p++ {
		c := NewFromSize(1000, p)
		i, j, f := c.computeHashPositionsAndFingerprint([]byte("Hello World"))
		if alt := c.getAlternativePosition(i, f); alt != j {
			t.Errorf("Get altertative position has fail (in p = %d), expected position was %d, gotten position is %d", p, j, alt)
		}
		if alt := c.getAlternativePosition(j, f); alt != i {
			t.Errorf("Get altertative position has fail (in p = %d), expected position was %d, gotten position is %d", p, i, alt)
		}
	}
}

func TestNewFromSize(t *testing.T) {
	cFList := []CuckooFilter{
		{
			n: 100,
			m: 32,
			p: defaultP,
		},
		{
			n: 1000,
			m: 512,
			p: defaultP,
		},
	}
	for _, expectedCF := range cFList {
		cf := NewFromSize(expectedCF.n)
		if cf.n != expectedCF.n {
			t.Errorf("Expected capacity %d, Current capacity %d", expectedCF.n, cf.n)
		}
		if cf.m != expectedCF.m {
			t.Errorf("Expected size %d, Current size %d", expectedCF.m, cf.m)
		}
	}
}

func TestInsertAndLookupAndDelete(t *testing.T) {
	c := NewFromSize(1000)
	elem1 := []byte("Hello World")
	elem2 := []byte("A")
	if ok := c.Insert(elem1); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem1)
	}
	if ok := c.Insert(elem2); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem2)
	}
	if ok := c.Lookup(elem1); !ok {
		t.Errorf("%s should be in.", elem1)
	}
	if ok := c.Lookup(elem2); !ok {
		t.Errorf("%s should be in.", elem2)
	}
	if ok := c.Delete(elem2); !ok {
		t.Errorf("%s should be deleted.", elem2)
	}
	if ok := c.Lookup(elem2); ok {
		t.Errorf("%s should not be in.", elem2)
	}
}

func TestItFailsOnInsertMoreThan2bRepeatedElements(t *testing.T) {
	c := NewFromSize(100)
	elem1 := []byte("Hello World")
	for i := 0; i < 9; i++ {
		ok := c.Insert(elem1)
		if !ok && i != 8 {
			t.Errorf("%s NOT correctly inserted in.", elem1)
		}
		if ok && i == 8 {
			t.Errorf("%s should NOT be correctly inserted in.", elem1)
		}
	}
}

func TestInsertAndLookupWithHighLoadFactor(t *testing.T) {
	size := uint(1000000)
	c := NewFromSize(size)
	expectedError := c.computeError()
	listElement := make([]string, size)
	for i := uint(0); i < size; i++ {
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := c.Insert([]byte(listElement[i])); !ok {
			t.Errorf("%s NOT correctly inserted.", listElement[i])
		}
	}
	// Look up for elements already in the filter
	for _, elem := range listElement {
		if ok := c.Lookup([]byte(elem)); !ok {
			t.Errorf("%s should be in.", elem)
		}
	}
	// Look up for elements that NOT exists in the filter
	elementsToTest := 20000
	falsePositives := 0
	for i := uint(0); i < uint(elementsToTest); i++ {
		toInsert := i + size + 1
		elem := fmt.Sprintf("%d", toInsert)
		if ok := c.Lookup([]byte(elem)); ok {
			falsePositives++
		}
	}

	expectedFalsePositives := int(float64(elementsToTest) * expectedError)
	rangeFalsePositives := int(math.Ceil(float64(expectedFalsePositives)*errorRangeFalsePositives) + 1)
	currentRange := falsePositives - expectedFalsePositives
	if currentRange > 0 && currentRange > rangeFalsePositives {
		t.Errorf("Error: Expected false positives are %d ± %d and current false positives are %d", expectedFalsePositives, rangeFalsePositives, falsePositives)
	}
}

func TestInsertAndLookupAndDeleteWithHighLoadFactor(t *testing.T) {
	size := uint(100000)
	c := NewFromSize(size)
	listElement := make([]string, size)
	for i := uint(0); i < size; i++ {
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := c.Insert([]byte(listElement[i])); !ok {
			t.Errorf("%s NOT correctly inserted.", listElement[i])
		}
	}
	// Delete 75% od elements
	for i := uint(0); i < size; i++ {
		if i%4 == 0 {
			continue
		}
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := c.Delete([]byte(listElement[i])); !ok {
			t.Errorf("%s should be deleted in.", listElement[i])
		}
	}
	// Check if elements are correctly deleted
	elementsToTest := int(size)
	falsePositives := 0
	for i := uint(0); i < size; i++ {
		if i%4 == 0 {
			if ok := c.Lookup([]byte(listElement[i])); !ok {
				t.Errorf("%s should be in.", listElement[i])
			}
			continue
		}
		if ok := c.Lookup([]byte(listElement[i])); ok {
			falsePositives++
		}
	}
	expectedFalsePositives := int(float64(elementsToTest) * c.computeError())
	rangeFalsePositives := int(math.Ceil(float64(expectedFalsePositives)*errorRangeFalsePositives) + 1)
	currentRange := falsePositives - expectedFalsePositives
	if currentRange > 0 && currentRange > rangeFalsePositives {
		t.Errorf("Error: Expected false positives are %d ± %d and current false positives are %d", expectedFalsePositives, rangeFalsePositives, falsePositives)
	}

}
