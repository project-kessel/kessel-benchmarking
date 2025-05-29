package regular_tests

import (
	"database/sql"
	"fmt"
	"github.com/yourusername/go-db-bench/benchmark"
	"github.com/yourusername/go-db-bench/config"
	"gorm.io/gorm"
	"runtime"
	"testing"
	"time"
)

func TestNormalizedRefs2RepTables(t *testing.T) {

	db := config.ConnectDB()
	if err := benchmark.ResetDatabase(db); err != nil {
		t.Fatalf("failed to reset DB: %v", err)
	}
	records, err := benchmark.LoadInputRecords("input_files/input_10000_records.jsonl")
	if err != nil {
		t.Fatalf("failed to load input records: %v", err)
	}

	//Start timer
	start := time.Now()
	// Start memory profiling
	var memStatsStart, memStatsEnd runtime.MemStats
	runtime.ReadMemStats(&memStatsStart)

	for i, rec := range records {
		err := db.Transaction(func(tx *gorm.DB) error {
			return benchmark.ProcessRecordOption3(tx, rec)
		}, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			t.Errorf("record %d transaction failed: %v", i, err)
		}

		// Optional: print progress every 10k
		if (i+1)%10000 == 0 {
			fmt.Printf("Processed %d records...\n", i+1)
		}
	}

	// End profiling
	runtime.ReadMemStats(&memStatsEnd)
	elapsed := time.Since(start)

	// Summary
	fmt.Printf("📊 Processed %d records in %s\n", len(records), elapsed)
	fmt.Printf("🧠 Memory Used: %.2f MB\n", float64(memStatsEnd.Alloc-memStatsStart.Alloc)/1024/1024)
	fmt.Printf("🔼 Total Allocated: %.2f MB\n", float64(memStatsEnd.TotalAlloc)/1024/1024)
	fmt.Printf("💾 Num GC: %d\n", memStatsEnd.NumGC)
}
