package cuckooFilter

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
	n := uint(len(usernames))
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
			f := NewFromSizeAndError(n, pr.e)
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
	resultsFile, err := os.Create(fmt.Sprintf("../results/CF_Insert_n:%d.csv", n))
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

	for i := uint(0); i < n; i++ {
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
	n := uint(len(usernames))
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
			f := NewFromSizeAndError(n, pr.e)
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
	resultsFile, err := os.Create(fmt.Sprintf("../results/CF_Lookup_n:%d.csv", n))
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

	for i := uint(0); i < n; i++ {
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

func TestThroughputDelete(t *testing.T) {
	usernames, err := utils.ReadDataset()
	if err != nil {
		t.Fatal(err)
	}
	n := uint(len(usernames))
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
			f := NewFromSizeAndError(n, pr.e)
			for _, user := range usernames {
				ok := f.Insert(user)
				if !ok {
					t.Fatal("Insertion has fail")
				}
			}
			for j, user := range usernames {
				start := time.Now()
				ok := f.Delete(user)
				elapsed := time.Since(start).Nanoseconds()
				if !ok {
					t.Fatal("element should be deleted")
				}
				results[k][j] += elapsed
			}
		}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/CF_Delete_n:%d.csv", n))
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

	for i := uint(0); i < n; i++ {
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


func TestFPRateWhileInserting(t *testing.T) {
	datasetSize := 126000
	usernames, err := utils.ReadDatasetFromCsvAndFixLengthTo(datasetSize)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
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

	n := 110000
	bunch := 1000
	results := make([][]float64, len(proofs))
	for k, pr := range proofs {
		results[k] = make([]float64, datasetSize/bunch)
		insertPoint := 0
		f := NewFromSizeAndError(uint(n), pr.e)
		for insertPoint < datasetSize {
			aux := 0
			for aux < bunch && insertPoint < datasetSize {
				ok := f.Insert(usernames[insertPoint])
				if !ok {
					t.Fatalf("insertion has fail in insertion %d, when load factor is %f", insertPoint, float64(insertPoint)/float64(f.m * b))
				}
				insertPoint++
				aux++
			}
			for j := 0; j < arithmeticMean; j++ {
				falsePositives := 0
				lookupDataset, _ := utils.CreateRandomDataset()
				for _, elem := range lookupDataset {
					ok := f.Lookup(elem)
					if ok {
						falsePositives++
					}
				}
				results[k][insertPoint/bunch-1] += float64(falsePositives)/float64(len(lookupDataset))
			}
		}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/CF_FP_n:%d_bunch:%d_1.csv", n, bunch))
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

	for i := uint(0); i < uint(len(results[0])); i++ {
		keep := make([]string, len(results))
		for j := 0; j < len(results); j++ {
			keep[j] = fmt.Sprint(results[j][i])
		}
		err = w.Write(keep)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}

func TestFPRateWhenFilterIsAtMaxAllowedCapacity(t *testing.T) {
	datasetSize := 110000
	usernames, err := utils.ReadDatasetFromCsvAndFixLengthTo(datasetSize)
	if err != nil {
		t.Fatal(err)
	}
	if err != nil {
		t.Fatal(err)
	}
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
	results := make([][]float64, len(proofs))
	lenP := 20
	for k, pr := range proofs {
		results[k] = make([]float64, lenP)
		f := NewFromSizeAndError(uint(datasetSize), pr.e)
		fmt.Println(f.m, f.p)
		for i, user := range usernames {
			ok := f.Insert(user)
			if !ok {
				t.Fatalf("insertion has fail in insertion %d, when load factor is %f", i, float64(i)/float64(f.m * b))
			}
		}
		for j := 0; j < arithmeticMean; j++ {
			for i := 0; i < lenP; i++ {
				lookupDataset, _ := utils.CreateRandomDataset()
				falsePositives := 0
				for _, elem := range lookupDataset {
					ok := f.Lookup(elem)
					if ok {
						falsePositives++
					}
				}
				results[k][i] += float64(falsePositives)/float64(len(lookupDataset))
			}
		}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/CF_FP_n:%d_2.csv", datasetSize))
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

	for i := uint(0); i < uint(len(results[0])); i++ {
		keep := make([]string, len(results))
		for j := 0; j < len(results); j++ {
			keep[j] = fmt.Sprint(results[j][i]/float64(arithmeticMean))
		}
		err = w.Write(keep)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}
