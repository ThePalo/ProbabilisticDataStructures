package quotientFilter

import (
	"ProbabilisticDataStructures/utils"
	"encoding/csv"
	"fmt"
	"os"
	"testing"
	"time"
)

const arithmeticMean = 10

type proof struct {
	e float64
}

func TestThroughputInsert(t *testing.T) {
	usernames, err := utils.ReadDataset()
	if err != nil {
		t.Fatal(err)
	}
	n := len(usernames)
	proofs := []proof {
		{
			e: 0.03,
		},
		{
			e: 0.001,
		},
		{
			e: 0.0001,
		},
	}

	results := make([][]int64, len(proofs))
	for k, pr := range proofs {
		results[k] = make([]int64, n)
		for i := 0; i < arithmeticMean; i++ {
			f := NewFromSizeAndError(uint(n), pr.e)
			for j, user := range usernames {
				start := time.Now()
				ok := f.Insert(user)
				elapsed := time.Since(start).Nanoseconds()
				if !ok {
					t.Fatal("Insertion has fail")
				}
				results[k][j] += elapsed
			}
		}
	}
	resultsFile, err := os.Create("../results/QF_Insert.csv")
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(resultsFile)

	//Title
	title := make([]string, len(proofs))
	for k := range results {
		title[k] = fmt.Sprint(proofs[k].e)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = w.Write(title)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		keep := make([]string, len(results))
		for j := 0; j < len(results); j++ {
			keep[j] = fmt.Sprint(results[j][i]/arithmeticMean)
		}
		err = w.Write(keep)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}

func TestThroughputLookup(t *testing.T) {
	usernames, err := utils.ReadDataset()
	if err != nil {
		t.Fatal(err)
	}
	n := len(usernames)
	proofs := []proof {
		{
			e: 0.03,
		},
		{
			e: 0.001,
		},
		{
			e: 0.0001,
		},
	}

	results := make([][]int64, len(proofs))
	for k, pr := range proofs {
		results[k] = make([]int64, n)
		for i := 0; i < arithmeticMean; i++ {
			f := NewFromSizeAndError(uint(n), pr.e)
			for _, user := range usernames {
				ok := f.Insert(user)
				if !ok {
					t.Fatal("Insertion has fail")
				}
			}
			for j, user := range usernames {
				start := time.Now()
				ok := f.Lookup(user)
				elapsed := time.Since(start).Nanoseconds()
				if !ok {
					t.Fatal("element should be in")
				}
				results[k][j] += elapsed
			}
		}
	}
	resultsFile, err := os.Create("../results/QF_Lookup.csv")
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(resultsFile)

	//Title
	title := make([]string, len(proofs))
	for k := range results {
		title[k] = fmt.Sprint(proofs[k].e)
		if err != nil {
			t.Fatal(err)
		}
	}
	err = w.Write(title)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < n; i++ {
		keep := make([]string, len(results))
		for j := 0; j < len(results); j++ {
			keep[j] = fmt.Sprint(results[j][i]/arithmeticMean)
		}
		err = w.Write(keep)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}