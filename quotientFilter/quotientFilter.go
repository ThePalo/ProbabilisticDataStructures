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
	n     uint
	m     uint
	q     uint
	r     uint
	e     float64
	slots []slot
}

func newQF(n uint, m uint, q uint, r uint, e float64) QuotientFilter {
	if q+r > 64 {
		//panic("Error")
	}
	return QuotientFilter{
		n:     n,
		e:     e,
		m:     m,
		r:     r,
		q:     q,
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
	r := computeSizeR(n, q, e)
	return newQF(n, m, q, r, e)
}

func (q *QuotientFilter) Insert(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.insert(fq, fr)
}

func (q QuotientFilter) Lookup(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.lookup(fq, fr)
}

func (q *QuotientFilter) Delete(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.delete(fq, fr)
}

func (q QuotientFilter) Print() {
	for _, slot := range q.slots {
		s := "empty"
		if slot.getReminder() != nil {
			s = fmt.Sprint(utils.BitSetToUint(slot.getReminder()))
		}
		fmt.Printf("  %t\t| %t\t| %t\t| => %s\n", slot.isOccupied, slot.isContinuation, slot.isShifted, s)
	}
	fmt.Println()
}

func (q *QuotientFilter) insert(fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied && q.slots[fq].isEmpty() {
		q.slots[fq].add(fr)
		q.slots[fq].isOccupied = true
		return true
	}
	wasOccupied := q.slots[fq].isOccupied
	q.slots[fq].isOccupied = true
	start, end := q.scan(fq)
	i := start
	for i != end {
		if q.slots[i].isEmpty() {
			q.slots[i].add(fr)
			// Set shifted bit if canonical slot is already in use or not
			if i != fq {
				q.slots[i].isShifted = true
			} else {
				q.slots[i].isShifted = false
			}
			return true
		}
		if q.slots[i].getReminder().Equal(fr) {
			return true
		}
		if utils.BitSetToUint(q.slots[i].getReminder()) > utils.BitSetToUint(fr) {
			break
		}
		i = q.next(i)
	}
	// This element becomes the beginning of a new run
	if !wasOccupied {
		q.shiftRight(i)
		// New element becomes the beginning of the run
		q.slots[i].add(fr)
		q.slots[i].isContinuation = false
	} else if i == start {
		// Old start of run becomes a continuation
		q.slots[i].isContinuation = true
		q.slots[i].isShifted = true
		q.shiftRight(i)
		// New element becomes the beginning of the run
		q.slots[i].add(fr)
		q.slots[i].isContinuation = false
	} else {
		// New element becomes a continuation
		q.shiftRight(i)
		q.slots[i].add(fr)
		q.slots[i].isContinuation = true
	}
	// Set shifted bit if canonical slot is already in use or not
	if i != fq {
		q.slots[i].isShifted = true
	} else {
		q.slots[i].isShifted = false
	}
	return true
}

func (q *QuotientFilter) shiftRight(pos uint) {
	prev := q.slots[pos].getSlot()
	i := q.next(pos)
	for {
		curr := q.slots[i].getSlot()
		if q.slots[i].isEmpty() {
			q.slots[i].add(prev.reminder)
			if prev.isInitRun() {
				q.slots[i].isContinuation = prev.isContinuation
				q.slots[i].isShifted = true
			} else {
				q.slots[i].isContinuation = prev.isContinuation
				q.slots[i].isShifted = prev.isShifted
			}
			return
		}
		q.slots[i].add(prev.reminder)
		if prev.isInitRun() {
			q.slots[i].isContinuation = prev.isContinuation
			q.slots[i].isShifted = true
		} else {
			q.slots[i].isContinuation = prev.isContinuation
			q.slots[i].isShifted = prev.isShifted
		}
		i = q.next(i)
		prev = curr.getSlot()
	}
}

func (q QuotientFilter) lookup(fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied {
		return false
	}
	start, end := q.scan(fq)
	i := start
	for i != end {
		if q.slots[i].getReminder().Equal(fr) {
			return true
		}
		i = q.next(i)
	}
	return false
}

func (q *QuotientFilter) delete(fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied {
		return true
	}
	start, end := q.scan(fq)
	fmt.Printf("Element: %d\tstart: %d\tend: %d\n", utils.BitSetToUint(fr), start, end)
	i := start
	for i != end {
		if q.slots[i].getReminder().Equal(fr) {
			q.slots[i].delete()
			wasInitRun := q.slots[i].isInitRun()
			wasShifted := q.slots[i].isShifted
			q.shiftLeft(i, fq)
			// Run with one element
			if start == q.prev(end) {
				q.slots[fq].isOccupied = false
			}
			if wasInitRun {
				// If exist a continuation, it becomes init of cluster/run
				if q.slots[i].isContinuation {
					q.slots[i].isContinuation = false
					q.slots[i].isShifted = wasShifted
				}
			}
			return true
		}
		i = q.next(i)
	}
	return false
}

func (q *QuotientFilter) shiftLeft(pos uint, canonicalSlot uint) {
	i := q.next(pos)
	for {
		if q.slots[i].isEmpty() || q.slots[i].isInitCluster() {
			q.slots[q.prev(i)].add(nil)
			q.slots[q.prev(i)].isContinuation = false
			q.slots[q.prev(i)].isShifted = false
			return
		}
		q.slots[q.prev(i)].add(q.slots[i].getReminder())
		q.slots[q.prev(i)].isContinuation = q.slots[i].isContinuation
		q.slots[q.prev(i)].isShifted = q.slots[i].isShifted
		// If init run, shifted bit must be unset if new slot is canonical
		if q.slots[i].isInitRun() {
			for ok := true; ok; ok = !q.slots[canonicalSlot].isOccupied {
				canonicalSlot = q.next(canonicalSlot)
			}
			if q.slots[q.prev(i)].isOccupied && canonicalSlot == q.prev(i) {
				q.slots[q.prev(i)].isShifted = false
			}
		}
		i = q.next(i)
	}
}

// scan run of fq such that the run is [start, end)
func (q QuotientFilter) scan(fq uint) (uint, uint) {
	j := fq
	for q.slots[j].isShifted {
		j = q.prev(j)
	}
	start := j
	for j != fq {
		for ok := true; ok; ok = q.slots[start].isContinuation {
			start = q.next(start)
		}
		for ok := true; ok; ok = !q.slots[j].isOccupied {
			j = q.next(j)
		}
	}
	end := start
	for ok := true; ok; ok = q.slots[end].isContinuation {
		end = q.next(end)
	}
	return start, end
}

func getFingerprint(element []byte) uint64 {
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

func (q QuotientFilter) next(i uint) uint {
	return (i + 1) % q.m
}

func (q QuotientFilter) prev(i uint) uint {
	return (i - 1) % q.m
}

func computeSizeQ(size uint) uint {
	return uint(math.Ceil(math.Log2(float64(size) / loadFactor)))
}

func computeSizeN(m uint) uint {
	return uint(float64(m) * loadFactor)
}

func computeError(n, p uint) float64 {
	return float64(n) / math.Pow(2, float64(p))
}

func computeSizeR(size uint, q uint, error float64) uint {
	return uint(math.Ceil(math.Log2(-float64(size) / (math.Pow(2, float64(q)) * math.Log(1.0-error)))))
}

func computeSizeM(sizeQ uint) uint {
	return uint(math.Pow(2, float64(sizeQ)))
}