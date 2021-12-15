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
	return QuotientFilter{
		n: n,
		e: e,
		m: m,
		r: r,
		q: q,
		slots: func() []slot {
			slots := make([]slot, m)
			for i := range slots {
				slots[i].metadata = bitset.New(metadataSize)
			}
			return slots
		}(),
	}
}

func New(q, r uint) QuotientFilter {
	if q+r > 64 {
		panic("Error")
	}
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
	if !q.slots[fq].isOccupied() && q.slots[fq].isEmpty() {
		q.slots[fq].add(fr, q.q)
		q.slots[fq].setOccupied()
		return true
	}
	q.slots[fq].setOccupied()
	start, end := q.scan(fq)
	fmt.Println(start, end)
	for i := start; i <= end; i++ {
		if q.slots[i].get() == fr {
			return true
		} else if utils.BitSetToUint(q.slots[i].get()) > utils.BitSetToUint(fr) {
			q.shiftRight(i)
			q.slots[i].add(fr, q.q)
			return true
		}
	}
	q.shiftRight(end+1)
	q.slots[end+1].add(fr, q.q)
	return true
}

func (q* QuotientFilter) Lookup(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	if !q.slots[fq].isOccupied() {
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
	if !q.slots[fq].isOccupied() {
		return true
	}
	start, end := q.scan(fq)
	fmt.Println(start, end)
	for i := start; i <= end; i++ {
		if q.slots[i].get().Equal(fr) {
			q.slots[i].delete()
			if start == end {
				q.slots[i].setTo(isOccupied, false)
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
			q.slots[i].add(prev.get(), q.q)
			q.slots[i].setTo(isContinuation, true)
			q.slots[i].setTo(isShifted, true)
			return
		}
		curr := q.slots[i]
		q.slots[i].add(prev.get(), q.q)
		q.slots[i].setTo(isContinuation, prev.isContinuation())
		q.slots[i].setTo(isShifted, prev.isShifted())
		prev.add(curr.get(), q.q)
		prev.setTo(isContinuation, curr.isContinuation())
		prev.setTo(isShifted, curr.isShifted())
		i = (i+1)%q.m
	}
}

func (q* QuotientFilter) shiftLeft(pos uint) {
	i := (pos + 1)%q.m
	for q.slots[i].get() != nil {
		q.slots[i-1].add(q.slots[i].get(), q.q)
		q.slots[i-1].setTo(isContinuation, q.slots[i].isContinuation())
		q.slots[i-1].setTo(isShifted, q.slots[i].isShifted())
		q.slots[i].add(nil, q.q)
		q.slots[i].setTo(isContinuation, false)
		q.slots[i].setTo(isShifted, false)
		i = (i+1)%q.m
	}
}


func (q QuotientFilter) scan(fp uint) (uint, uint) {
	pos := fp
	j := pos
	for q.slots[j].isShifted() {
		j--
	}
	start := pos
	for j != pos {
		for ok := true; ok; ok = q.slots[start].isContinuation() {
			start++
		}
		for ok := true; ok; ok = !q.slots[j].isOccupied() {
			j++
		}
	}
	end := start
	for ok := true; ok; ok = q.slots[end].isContinuation() {
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
	fp := bitset.New(q.q)
	for i := uint(0); i < q.q; i++ {
		pos := q.r + i
		fp.SetTo(i, fingerprint.Test(pos))
	}
	return utils.BitSetToUint(fp), fr
}

func (q QuotientFilter) Print() {
	for _, slot := range q.slots {
		fmt.Printf("%t\t| %t\t| %t\t| => %v\n", slot.isOccupied(), slot.isContinuation(), slot.isShifted(), slot.get() )
	}
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