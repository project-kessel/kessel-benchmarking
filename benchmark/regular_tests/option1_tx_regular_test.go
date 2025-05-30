package regular_tests

import (
	"database/sql"
	"fmt"
	"github.com/yourusername/go-db-bench/benchmark"
	"github.com/yourusername/go-db-bench/config"
	"gorm.io/gorm"
	"sort"
	"testing"
	"time"
)

var runCount = 5

const (
	outputCSVPath = "benchmark_results.csv" // Change if needed
)

func TestDenormalizedRefs2RepTables(t *testing.T) {
	startFreshCSVFile := true

	for run := 1; run <= runCount; run++ {
		if run > 1 {
			startFreshCSVFile = false
		}
		fmt.Printf("\n🔁 Starting run %d/%d\n", run, runCount)

		db := config.ConnectDB()
		if err := benchmark.ResetDatabase(db); err != nil {
			t.Fatalf("failed to reset DB: %v", err)
		}

		records, err := benchmark.LoadInputRecords("/Users/snehagunta/git/kessel/kessel-benchmarking/benchmark/input_files/input_10_records.jsonl")
		if err != nil {
			t.Fatalf("failed to load input records: %v", err)
		}

		durations := make([]time.Duration, 0, len(records))
		startTotal := time.Now()

		for i, rec := range records {
			start := time.Now()

			err := db.Transaction(func(tx *gorm.DB) error {
				return benchmark.ProcessRecordOption1(tx, rec)
			}, &sql.TxOptions{Isolation: sql.LevelSerializable})

			duration := time.Since(start)
			durations = append(durations, duration)

			if err != nil {
				t.Errorf("record %d transaction failed: %v", i, err)
			}
		}

		totalElapsed := time.Since(startTotal)

		sort.Slice(durations, func(i, j int) bool { return durations[i] < durations[j] })
		getPercentile := func(p float64) time.Duration {
			index := int(float64(len(durations)) * p)
			if index >= len(durations) {
				index = len(durations) - 1
			}
			return durations[index]
		}

		p50 := getPercentile(0.50)
		p90 := getPercentile(0.90)
		p99 := getPercentile(0.99)
		maxTime := durations[len(durations)-1]

		fmt.Printf("\n📊 Run %d: Processed %d records in %s\n", run, len(records), totalElapsed)
		fmt.Printf("⏱️ Per-record latency:\n")
		fmt.Printf("  - p50: %s\n", p50)
		fmt.Printf("  - p90: %s\n", p90)
		fmt.Printf("  - p99: %s\n", p99)
		fmt.Printf("  - maxTime: %s\n", maxTime)

		benchmark.WriteCSV(totalElapsed, p50, p90, p99, maxTime, len(records), outputCSVPath, startFreshCSVFile)
	}
}
