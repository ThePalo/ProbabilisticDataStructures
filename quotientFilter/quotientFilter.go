package quotientFilter

import (
	"ProbabilisticDataStructures/utils"
	"fmt"
	"math"

	"github.com/bits-and-blooms/bitset"
	"github.com/spaolacci/murmur3"
)

const (
	loadFactor = 0.65
)

type QuotientFilter struct {
	n uint
	m uint
	q uint
	r uint
	e float64
	slots []slot
}



func newQF(n uint, m uint, q uint, r uint, e float64) QuotientFilter {
	if q+r > 64 {
		//panic("Error")
	}
	return QuotientFilter{
		n: n,
		e: e,
		m: m,
		r: r,
		q: q,
		slots: make([]slot, m),
	}
}

func New(q, r uint) QuotientFilter {
	m := computeSizeM(q)
	n := computeSizeN(m)
	e := computeError(n, r+q)
	return newQF(n, m, q, r, e)
}

func NewFromSizeAndError(n uint, e float64) QuotientFilter {
	q := computeSizeQ(n)
	m := computeSizeM(q)
	r := computeSizeR(n, m, e)
	return newQF(n, m, q, r, e)
}


func (q* QuotientFilter) Insert(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.insert(fq, fr)
}

func (q* QuotientFilter) insert (fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied && q.slots[fq].isEmpty() {
		q.slots[fq].add(fr, q.r)
		q.slots[fq].isOccupied = true
		return true
	}
	q.slots[fq].isOccupied = true
	start, end := q.scan(fq)
	fmt.Printf("%d\n\tstart: %d\tend: %d\n", fq, start, end)
	for i := start; i < end; i++ {
		if q.slots[i].get().Equal(fr) {
			return true
		}
		if utils.BitSetToUint(q.slots[i].get()) > utils.BitSetToUint(fr) {
			q.shiftRight(i)
			q.slots[i].add(fr, q.r)
			if i != start {
				q.slots[i].isContinuation = true
			}
			if i != fq {
				q.slots[i].isShifted = true
			}
			return true
		}
	}
	q.shiftRight(end)
	q.slots[end].add(fr, q.r)
	if end != start {
		q.slots[end].isContinuation = true
	}
	if end != fq {
		q.slots[end].isShifted = true
	}
	return true
}

func (q* QuotientFilter) Lookup(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	if !q.slots[fq].isOccupied {
		return false
	}
	start, end := q.scan(fq)
	for i := start; i <= end; i++ {
		if q.slots[i].get().Equal(fr) {
			return true
		}
	}
	return false
}

func (q* QuotientFilter) Delete(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	if !q.slots[fq].isOccupied {
		return true
	}
	start, end := q.scan(fq)
	for i := start; i <= end; i++ {
		if q.slots[i].get().Equal(fr) {
			q.slots[i].delete()
			if start == end {
				q.slots[i].isOccupied = false
			} else if i < end {
				q.shiftLeft(i+1)
			}
			return true
		}
	}
	return false
}



func (q* QuotientFilter) shiftRight(pos uint) {
	prev := q.slots[pos]
	i := (pos + 1)%q.m
	for {
		if q.slots[i].isEmpty() {
			q.slots[i].add(prev.get(), q.r)
			q.slots[i].isContinuation = true
			q.slots[i].isShifted = true
			return
		}
		curr := slot{
			isOccupied: q.slots[i].isOccupied,
			isContinuation: q.slots[i].isContinuation,
			isShifted: q.slots[i].isShifted,
		}
		curr.add(q.slots[i].get(), q.r)

		q.slots[i].add(prev.get(), q.r)
		q.slots[i].isContinuation = prev.isContinuation
		q.slots[i].isShifted = prev.isShifted

		prev.add(curr.get(), q.r)
		prev.isContinuation = curr.isContinuation
		prev.isShifted = curr.isShifted
		i = (i+1)%q.m
	}
}

func (q* QuotientFilter) shiftLeft(pos uint) {
	i := (pos + 1)%q.m
	for q.slots[i].get() != nil {
		q.slots[i-1].add(q.slots[i].get(), q.r)
		q.slots[i-1].isContinuation = q.slots[i].isContinuation
		q.slots[i-1].isShifted = q.slots[i].isShifted
		q.slots[i].add(nil, q.r)
		q.slots[i].isContinuation = false
		q.slots[i].isShifted = false
		i = (i+1)%q.m
	}
}

// scan run of fq such that the run is [start, end)
func (q QuotientFilter) scan(fq uint) (uint, uint) {
	j := fq
	for q.slots[j].isShifted {
		j = (j-1)%q.m
	}
	start := j
	for j != fq {
		for ok := true; ok; ok = q.slots[start].isContinuation {
			start++
		}
		for ok := true; ok; ok = !q.slots[j].isOccupied {
			j++
		}
	}
	end := start
	for ok := true; ok; ok = q.slots[end].isContinuation {
		end++
	}
	return start, end
}

func getFingerprint(element []byte) uint64{
	return murmur3.Sum64(element)
}

func (q QuotientFilter) getQuotientPosAndRest(f uint64) (uint, *bitset.BitSet) {
	fingerprint := bitset.From([]uint64{f})
	fr := bitset.New(q.r)
	for i := uint(0); i < q.r; i++ {
		fr.SetTo(i, fingerprint.Test(i))
	}
	fq := bitset.New(q.q)
	for i := uint(0); i < q.q; i++ {
		pos := q.r + i
		fq.SetTo(i, fingerprint.Test(pos))
	}
	return utils.BitSetToUint(fq), fr
}

func (q QuotientFilter) Print() {
	for _, slot := range q.slots {
		s := "empty"
		if slot.get() != nil {
			s = fmt.Sprint(utils.BitSetToUint(slot.get()))
		}
		fmt.Printf("%t\t| %t\t| %t\t| => %s\n", slot.isOccupied, slot.isContinuation, slot.isShifted, s)
	}
	fmt.Println()
}


func computeSizeQ(size uint) uint {
	return uint(math.Ceil(math.Log2(float64(size) / loadFactor)))
}

func computeSizeN(m uint) uint {
	return uint(float64(m)*loadFactor)
}

func computeError(n, p uint) float64 {
	return float64(n)/math.Pow(2, float64(p))
}

func computeSizeR(size uint, m uint, error float64) uint {
	return uint(math.Ceil(math.Log10(-float64(size)/(float64(m)*math.Log(1.0-error)))))
}

func computeSizeM(sizeQ uint) uint {
	return uint(math.Pow(2, float64(sizeQ)))
}