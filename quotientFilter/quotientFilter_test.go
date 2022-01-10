package quotientFilter

import (
	"fmt"
	"math"
	"testing"

	"github.com/bits-and-blooms/bitset"
)

const roundTo = 100000
const errorRangeFalsePositives = 0.1

func TestNewFromSizeAndError(t *testing.T) {
	qFList := []QuotientFilter{
		{
			n: 1000,
			m: 2048,
			q: 11,
			r: 6,
			e: 0.01,
		},
	}
	for _, expectedQF := range qFList {
		qf := NewFromSizeAndError(expectedQF.n, expectedQF.e)
		if qf.n != expectedQF.n {
			t.Errorf("Expected capacity %d, Current capacity %d", expectedQF.n, qf.n)
		}
		if math.Round(qf.e*roundTo)/roundTo != math.Round(expectedQF.e*roundTo)/roundTo {
			t.Errorf("Expected error %.5f, Current error %.5f", expectedQF.e, qf.e)
		}
		if qf.m != expectedQF.m {
			t.Errorf("Expected size %d, Current size %d", expectedQF.m, qf.m)
		}
		if qf.q != expectedQF.q {
			t.Errorf("Expected quotient size %d, Current quotient size %d", expectedQF.q, qf.q)
		}
		if qf.r != expectedQF.r {
			t.Errorf("Expected reminder size %d, Current reminder size %d", expectedQF.r, qf.r)
		}
	}
}


func TestItCanHandleRepeatedElements(t *testing.T) {
	q := New(4, 8)
	elem := []byte("Same Element")
	ok := q.Insert(elem)
	if !ok {
		t.Errorf("%s NOT correctly inserted in.", elem)
	}
	ok = q.Insert(elem)
	if !ok {
		t.Errorf("%s NOT correctly inserted in.", elem)
	}
	ok = q.Delete(elem)
	if !ok {
		t.Errorf("%s NOT correctly deleted from.", elem)
	}
	ok = q.Lookup(elem)
	if ok {
		t.Errorf("%s should NOT be in.", elem)
	}
}

func TestInsertAndLookupWithHighLoadFactor(t *testing.T) {
	size := uint(100000)
	e := 0.01
	q := NewFromSizeAndError(size, e)
	listElement := make([]string, size)
	for i := uint(0); i < size; i++ {
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := q.Insert([]byte(listElement[i])); !ok {
			t.Errorf("%s NOT correctly inserted.", listElement[i])
		}
	}

	// Look up for elements already in the filter
	for _, elem := range listElement {
		if ok := q.Lookup([]byte(elem)); !ok {
			t.Errorf("%s should be in.", elem)
		}
	}
	// Look up for elements that NOT exists in the filter
	elementsToTest := 2000000
	falsePositives := 0
	for i := uint(0); i < uint(elementsToTest); i++ {
		toInsert := i + size + 1
		elem := fmt.Sprintf("%d", toInsert)
		if ok := q.Lookup([]byte(elem)); ok {
			falsePositives++
		}
	}
	expectedFalsePositives := int(float64(elementsToTest) * e)
	rangeFalsePositives := int(math.Ceil(float64(expectedFalsePositives)*errorRangeFalsePositives) + 1)
	currentRange := falsePositives - expectedFalsePositives
	if currentRange > 0 && currentRange > rangeFalsePositives {
		t.Errorf("Error: Expected false positives are %d ± %d and current false positives are %d", expectedFalsePositives, rangeFalsePositives, falsePositives)
	}
}

func TestInsertAndLookupAndDeleteWithHighLoadFactor(t *testing.T) {
	size := uint(100000)
	e := 0.001
	q := NewFromSizeAndError(size, e)
	listElement := make([]string, size)
	for i := uint(0); i < size; i++ {
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := q.Insert([]byte(listElement[i])); !ok {
			t.Errorf("%s NOT correctly inserted.", listElement[i])
		}
	}
	falsePositives := 0
	elementsToTest := int(size)
	numberOfCollisions := 0
	// Delete 75% of elements
	for i := uint(0); i < size; i++ {
		if i%4 == 0 {
			continue
		}
		listElement[i] = fmt.Sprintf("%d", i)
		if ok := q.Delete([]byte(listElement[i])); !ok {
			numberOfCollisions++
		}
	}
	if numberOfCollisions > 0  {
		t.Logf("Delete has performed %d collisions", numberOfCollisions)
	}

	// Check if elements are correctly deleted
	elementsToTest = int(size)
	falsePositives = 0
	numberOfCollisions = 0
	for i := uint(0); i < size; i++ {
		if i%4 == 0 {
			if ok := q.Lookup([]byte(listElement[i])); !ok {
				numberOfCollisions++
			}
			continue
		}
		if ok := q.Lookup([]byte(listElement[i])); ok {
			falsePositives++
		}
	}
	expectedFalsePositives := int(float64(elementsToTest) * e)
	rangeFalsePositives := int(math.Ceil(float64(expectedFalsePositives)*errorRangeFalsePositives) + 1)
	currentRange := falsePositives - expectedFalsePositives
	if currentRange > 0 && currentRange > rangeFalsePositives {
		t.Errorf("Error: Expected false positives are %d ± %d and current false positives are %d", expectedFalsePositives, rangeFalsePositives, falsePositives)
	}
	if numberOfCollisions > 0  {
		t.Logf("Lookup after delete has performed %d collisions", numberOfCollisions)
	}

}

func TestInsertManually(t *testing.T) {
	q := New(3, 64)
	ok := q.insert(7, bitset.From([]uint64{71}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(3, bitset.From([]uint64{33}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
}

func TestInsertManuallyAndLookup(t *testing.T) {
	q := New(3, 64)
	ok := q.insert(7, bitset.From([]uint64{71}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	//LOOKUP
	ok = q.lookup(7, bitset.From([]uint64{71}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(1, bitset.From([]uint64{12}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(4, bitset.From([]uint64{41}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(1, bitset.From([]uint64{11}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
	ok = q.lookup(1, bitset.From([]uint64{10}))
	if !ok {
		t.Errorf("Element should be in the filter")
	}
}

func TestInsertManuallyAndDelete(t *testing.T) {
	q := New(3, 64)
	ok := q.insert(7, bitset.From([]uint64{71}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}), true)
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	//DELETE
	ok = q.delete(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("Element should be removed from the filter")
	}
	ok = q.delete(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("Element should be removed from the filter")
	}
}