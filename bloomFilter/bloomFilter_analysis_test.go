package bloomFilter

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
				f.Insert(user)
				elapsed := time.Since(start).Nanoseconds()
				results[k][j] += elapsed
			}
		}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/BF_Insert_n:%d.csv", n))
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
				f.Insert(user)
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
	resultsFile, err := os.Create(fmt.Sprintf("../results/BF_Lookup_n:%d.csv", n))
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
	usernames, err := utils.ReadDatasetFromCsvAndFixLengthTo150k()
	if err != nil {
		t.Fatal(err)
	}
	datasetSize := len(usernames)
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

	n := 130000
	bunch := 1000
	results := make([][]float64, len(proofs))
	for k, pr := range proofs {
		results[k] = make([]float64, datasetSize/bunch)
		insertPoint := 0
		f := NewFromSizeAndError(uint(n), pr.e)
		for insertPoint < datasetSize {
			aux := 0
			for aux < bunch && insertPoint < datasetSize {
				f.Insert(usernames[insertPoint])
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
	resultsFile, err := os.Create(fmt.Sprintf("../results/BF_FP_n:%d.csv", n))
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

var proofs = []proof{
	// Tenths
	{
		e: 0.10,
	},
	// Hundredths
	{
		e: 0.09,
	},
	{
		e: 0.08,
	},
	{
		e: 0.07,
	},
	{
		e: 0.06,
	},
	{
		e: 0.05,
	},
	{
		e: 0.04,
	},
	{
		e: 0.03,
	},
	{
		e: 0.02,
	},
	{
		e: 0.01,
	},
	// Thousandths
	{
		e: 0.009,
	},
	{
		e: 0.008,
	},
	{
		e: 0.007,
	},
	{
		e: 0.006,
	},
	{
		e: 0.005,
	},
	{
		e: 0.004,
	},
	{
		e: 0.003,
	},
	{
		e: 0.002,
	},
	{
		e: 0.001,
	},
	// Ten-thousandths
	{
		e: 0.0009,
	},
	{
		e: 0.0008,
	},
	{
		e: 0.0007,
	},
	{
		e: 0.0006,
	},
	{
		e: 0.0005,
	},
	{
		e: 0.0004,
	},
	{
		e: 0.0003,
	},
	{
		e: 0.0002,
	},
	{
		e: 0.0001,
	},
}

func TestSizeInFunctionOfErrorRate(t *testing.T) {
	n := 41943040
	results := make([][]string, len(proofs))
	for k, pr := range proofs {
		f := NewFromSizeAndError(uint(n), pr.e)
		results[k] = []string{fmt.Sprint(pr.e), fmt.Sprint(f.TotalSize())}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/BF_size_n:%d.csv", n))
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(resultsFile)

	//Title
	err = w.Write([]string{"error", "size"})
	if err != nil {
		t.Fatal(err)
	}

	for _, elem := range results {
		err = w.Write(elem)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}


func TestBitsPerSlotInFunctionOfErrorRate(t *testing.T) {
	n := 150000
	results := make([][]string, len(proofs))
	for k, pr := range proofs {
		f := NewFromSizeAndError(uint(n), pr.e)
		println(f.m, f.e, f.k)
		results[k] = []string{fmt.Sprint(pr.e), fmt.Sprint(f.k)}
	}
	resultsFile, err := os.Create(fmt.Sprintf("../results/BF_bits_per_element_n:%d.csv", n))
	if err != nil {
		t.Fatal(err)
	}
	w := csv.NewWriter(resultsFile)

	//Title
	err = w.Write([]string{"error", "size"})
	if err != nil {
		t.Fatal(err)
	}

	for _, elem := range results {
		err = w.Write(elem)
		if err != nil {
			t.Fatal(err)
		}
	}
	w.Flush()
	resultsFile.Close()
}