package cuckooFilter

import (
	"fmt"
	"math/rand"
	"testing"
)

func TestBasic(t *testing.T) {
	b := New(1000)
	elem1 := []byte("Hello World")
	elem2 := []byte("A")
	if ok := b.Insert(elem1); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem1)
	}
	if ok := b.Insert(elem2); !ok {
		t.Errorf("%s NOT correctly inserted in.", elem2)
	}
	if ok := b.Lookup(elem1); !ok {
		t.Errorf("%s should be in.", elem1)
	}
	if ok := b.Lookup(elem2); !ok {
		t.Errorf("%s should be in.", elem2)
	}
	if ok := b.Delete(elem2); !ok {
		t.Errorf("%s should be deleted in.", elem2)
	}
	if ok := b.Lookup(elem2); ok {
		t.Errorf("%s should not be in.", elem2)
	}
}

func TestItFailsOnInsertMoreThan2bRepeatedElements(t *testing.T) {
	b := New(1000)
	elem1 := []byte("Hello World")
	for i := 0; i < 9; i++ {
		ok := b.Insert(elem1)
		if !ok && i != 8 {
			t.Errorf("%s NOT correctly inserted in.", elem1)
		}
		if ok && i == 8 {
			t.Errorf("%s should NOT be correctly inserted in.", elem1)
		}
	}
}

func TestItCanHandleHighLoadFactor(t *testing.T) {
	b := New(980)
	elements := make([]string, 0)
	for i := 0; i < 980; i++ {
		num := rand.Int()
		s := fmt.Sprint(num)
		elements = append(elements, s)
		ok := b.Insert([]byte(s))
		if !ok {
			t.Errorf("%s NOT correctly inserted in.", s)
		}
	}
	numErrors := 0
	for i := 0; i < 980; i++ {
		ok := b.Lookup([]byte(elements[i]))
		if !ok {
			numErrors++
		}
	}
	ok := b.Lookup([]byte("elements[i]"))
	if !ok {
		numErrors++
	}
	fmt.Println(numErrors)
}