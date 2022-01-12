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
	count uint
	slots []slot
}

// New creates a new Quotient Filter with desired length (in bits) of quotient and reminder
func New(q, r uint) QuotientFilter {
	m := computeSizeM(q)
	n := computeSizeN(m)
	e := computeError(n, r+q)
	return newQF(n, m, q, r, e)
}

// NewFromSizeAndError creates a new Quotient Filter that can hold n elements with e false positive error
func NewFromSizeAndError(n uint, e float64) QuotientFilter {
	q := computeSizeQ(n)
	m := computeSizeM(q)
	r := computeSizeR(n, q, e)
	return newQF(n, m, q, r, e)
}

// Insert inserts element into QF. Expected computational time: O(1). Returns true if element has been inserted or already exists, false otherwise. Expected computational time: O(1)
func (q *QuotientFilter) Insert(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.insert(fq, fr, false)
}

// InsertUnique inserts element into QF if element is not already inserted. Returns true if element has been inserted or already exists, false otherwise. Expected computational time: O(1)
func (q *QuotientFilter) InsertUnique(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.insert(fq, fr, true)
}

// Lookup returns true if element may belong to the QF and false if element does not belong to the QF. Expected computational time: O(1)
func (q QuotientFilter) Lookup(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.lookup(fq, fr)
}

// Delete deletes element in the filter. Returns true if element has been deleted, false otherwise. Expected computational time: O(1)
func (q *QuotientFilter) Delete(element []byte) bool {
	f := getFingerprint(element)
	fq, fr := q.getQuotientPosAndRest(f)
	return q.delete(fq, fr)
}

// Print prints a representation of the current Quotient filter
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

func (q *QuotientFilter) insert(fq uint, fr *bitset.BitSet, unique bool) bool {
	if q.slots[fq].isEmpty() {
		q.slots[fq].add(fr)
		q.slots[fq].isOccupied = true
		return true
	}
	insertSlot := new(slot)
	insertSlot.add(fr)
	wasOccupied := q.slots[fq].isOccupied
	q.slots[fq].isOccupied = true
	start := q.scan(fq)
	i := start
	if wasOccupied {
		// Search for the position in the existing run
		for {
			if q.slots[i].getReminder().Equal(fr) {
				if unique {
					return true
				}
				break
			}
			if utils.BitSetToUint(q.slots[i].getReminder()) > utils.BitSetToUint(fr) {
				break
			}
			i = q.next(i)
			if !q.slots[i].isContinuation {
				break
			}
		}
		// Once having the desired position to insert the element into the run
		if i == start {
			// Old start of run becomes a continuation
			q.slots[i].isContinuation = true
			// New element becomes the beginning of the run
		} else {
			// New element becomes a continuation
			insertSlot.isContinuation = true
		}
	}
	// Set shifted bit if canonical slot is already in use or not
	if i != fq {
		insertSlot.isShifted = true
	} else {
		insertSlot.isShifted = false
	}
	q.shiftRightAndInsert(i, insertSlot)
	q.count++
	return true
}

func (q *QuotientFilter) shiftRightAndInsert (pos uint, insertSlot *slot) {
	var prev slot
	curr := insertSlot.getSlot()
	for {
		prev = q.slots[pos].getSlot()
		empty := prev.isEmpty()
		if !empty {
			prev.isShifted = true
			if prev.isOccupied {
				curr.isOccupied = true
				prev.isOccupied = false
			}
		}
		q.slots[pos] = curr.getSlot()
		curr = prev.getSlot()
		pos = q.next(pos)
		if empty {
			break
		}
	}
}

func (q QuotientFilter) lookup(fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied {
		return false
	}
	start := q.scan(fq)
	i := start
	for ok := true; ok; ok = q.slots[i].isContinuation {
		if q.slots[i].getReminder().Equal(fr) {
			return true
		}
		i = q.next(i)
	}
	return false
}

func (q *QuotientFilter) delete(fq uint, fr *bitset.BitSet) bool {
	if !q.slots[fq].isOccupied {
		return false
	}
	start := q.scan(fq)
	i := start
	for ok := true; ok; ok = q.slots[i].isContinuation {
		if q.slots[i].getReminder().Equal(fr) {
			break
		}
		if utils.BitSetToUint(q.slots[i].getReminder()) > utils.BitSetToUint(fr) {
			return false
		}
		i = q.next(i)
	}
	if q.slots[i].isEmpty() || !q.slots[i].getReminder().Equal(fr) {
		return false
	}

	wasInitRun := q.slots[i].isInitRun()
	wasShifted := q.slots[i].isShifted

	q.shiftLeft(i, fq)

	if wasInitRun {
		// If exist a continuation, it becomes init of cluster/run
		if q.slots[i].isContinuation {
			q.slots[i].isContinuation = false
			q.slots[i].isShifted = wasShifted
		} else {
			q.slots[fq].isOccupied = false
		}
	}
	return true
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
func (q QuotientFilter) scan(fq uint) uint {
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
	return start
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