package quotientFilter

import (
	"fmt"
	"testing"

	"github.com/bits-and-blooms/bitset"
)

func TestBasic(t *testing.T) {
	q := New(4, 8)
	elem1 := []byte("Hello World")
	if ok := q.Insert(elem1); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem1)
	}
	if ok := q.Lookup(elem1); !ok {
		t.Errorf("%s should be in.", elem1)
	}
	elem2 := []byte("AB")
	if ok := q.Insert(elem2); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem2)
	}
	if ok := q.Lookup(elem2); !ok {
		t.Errorf("%s should be in.", elem2)
	}
	if ok := q.Delete(elem2); !ok {
		t.Errorf("%s should be deleted in.", elem2)
	}
	if ok := q.Lookup(elem2); ok {
		t.Errorf("%s should not be in.", elem2)
	}
}

func TestItCanHandleRepeatedElements(t *testing.T) {
	q := New(4, 8)
	elem := []byte("Same Element")
	ok := q.Insert(elem)
	if !ok {
		t.Errorf("%s NOT correctly inserted in.", elem)
	}
	ok = q.Insert([]byte("Same Element"))
	if !ok {
		t.Errorf("%s NOT correctly inserted in.", elem)
	}
	q.Print()
}

func TestItCanHandleHighLoadFactor(t *testing.T) {
	q := New(4, 8)
	elements := make([]string, 0)
	for i := 4; i >= 0; i-- {
		s := fmt.Sprint(i)
		elements = append(elements, s)
		ok := q.Insert([]byte(s))
		if !ok {
			t.Errorf("%s NOT correctly inserted in.", s)
		}
	}
	/*numErrors := 0
	for i := 0; i < 10; i++ {
		ok := b.Lookup([]byte(elements[i]))
		if !ok {
			numErrors++
		}
	}
	ok := b.Lookup([]byte("elements[i]"))
	if !ok {
		numErrors++
	}
	fmt.Println(numErrors)*/
}

func TestInsertManually(t *testing.T) {
	q := New(3, 64)
	ok := q.insert(7, bitset.From([]uint64{71}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(3, bitset.From([]uint64{33}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	q.Print()
}

func TestInsertManuallyAndLookup(t *testing.T) {
	q := New(3, 64)
	ok := q.insert(7, bitset.From([]uint64{71}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	q.Print()
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
	ok := q.insert(7, bitset.From([]uint64{71}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{12}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(4, bitset.From([]uint64{41}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{11}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	ok = q.insert(1, bitset.From([]uint64{10}))
	if !ok {
		t.Errorf("NOT correctly inserted in")
	}
	q.Print()
	//DELETE
	ok = q.delete(2, bitset.From([]uint64{21}))
	if !ok {
		t.Errorf("Element should be removed from the filter")
	}
	ok = q.delete(2, bitset.From([]uint64{22}))
	if !ok {
		t.Errorf("Element should be removed from the filter")
	}
	q.Print()
}
