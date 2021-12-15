package quotientFilter

import (
	"fmt"
	"math/rand"
	"testing"
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

func TestItCanHandleHighLoadFactor(t *testing.T) {
	q := New(4, 8)
	elements := make([]string, 0)
	for i := 0; i < 9; i++ {
		num := rand.Int()
		s := fmt.Sprint(num)
		elements = append(elements, s)
		ok := q.Insert([]byte(s))
		if !ok {
			t.Errorf("%s NOT correctly inserted in.", s)
		}
	}
	q.Print()
	ok := q.Insert([]byte("543"))
	if !ok {
		t.Errorf("10 NOT correctly inserted in.")
	}
	/*numErrors := 0
	for i := 0; i < 10; i++ {
		ok := q.Lookup([]byte(elements[i]))
		if !ok {
			numErrors++
		}
	}
	ok := q.Lookup([]byte("elements[i]"))
	if !ok {
		numErrors++
	}
	fmt.Println(numErrors)*/
}