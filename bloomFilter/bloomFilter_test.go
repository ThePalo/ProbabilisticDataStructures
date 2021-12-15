package bloomFilter

import "testing"

func TestBasic(t *testing.T) {
	b := New(100, 0.03)
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
