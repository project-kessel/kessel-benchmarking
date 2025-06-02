package regular_tests

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/yourusername/go-db-bench/benchmark"
	"github.com/yourusername/go-db-bench/config"
	"gorm.io/gorm"
)

var runCount = 1

const inputRecordsPath = "input_10_records.jsonl"
const outputCSVPath = "per_run_results_option1_10.csv"
const outputPerRecordCSVPath = "per_record_results_option1_10.csv"

func TestDenormalizedRefs2RepTables(t *testing.T) {
	startFreshCSVFile := true
	cfg := config.LoadDBConfig()

	for run := 1; run <= runCount; run++ {
		if run > 1 {
			startFreshCSVFile = false
		}
		fmt.Printf("\n🔁 Starting run %d/%d\n", run, runCount)

		if err := config.DropAndRecreateDatabase(cfg); err != nil {
			t.Fatalf("❌ failed to reset DB: %v", err)
		}

		db := config.ConnectDB()

		// Reinitialize extensions if needed (like uuid-ossp or pgcrypto)
		// db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")

		// GORM auto-migration to recreate tables
		err := db.AutoMigrate(
			&models.Resource{},
			&models.CommonRepresentation{},
			&models.ReporterRepresentation{},
			&models.RepresentationReference{},
		)

		if err != nil {
			return
		}

		records, err := benchmark.LoadInputRecords("/Users/snehagunta/git/kessel/kessel-benchmarking/benchmark/input_files/" + inputRecordsPath)
		if err != nil {
			t.Fatalf("failed to load input records: %v", err)
		}

		durations := make([]time.Duration, 0, len(records))
		allStepTimings := [][]benchmark.StepTiming{}
		startTotal := time.Now()

		for i, rec := range records {
			start := time.Now()
			stepTimings, err := runInstrumentedTransaction(db, rec)
			duration := time.Since(start)

			durations = append(durations, duration)
			allStepTimings = append(allStepTimings, stepTimings)

			if err != nil {
				t.Errorf("record %d transaction failed: %v", i, err)
			}

			writeHeader := run == 1 && i == 0
			err = benchmark.WriteSingleRecordTimingCSV(run, i, duration, stepTimings, outputPerRecordCSVPath, writeHeader)
			if err != nil {
				t.Fatalf("failed to write CSV for record %d: %v", i, err)
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
		maxIndex := len(durations) - 1
		maxTime := durations[maxIndex]
		maxTimings := allStepTimings[maxIndex]

		var maxStep benchmark.StepTiming
		for _, step := range maxTimings {
			if step.Duration > maxStep.Duration {
				maxStep = step
			}
		}

		if maxStep.SQL != "" {
			//fmt.Printf("  - maxStep Vars:%s\n", maxStep.Vars)
			explainSQL := fmt.Sprintf("EXPLAIN %s", maxStep.SQL)
			rows, err := db.Raw(explainSQL, maxStep.Vars...).Rows()
			if err == nil {
				var lines []string
				for rows.Next() {
					var line string
					_ = rows.Scan(&line)
					lines = append(lines, line)
				}
				maxStep.Explain = strings.Join(lines, "\n")
			} else {
				fmt.Printf("  Cannot generate explain plan for max step: %s\n", err)
			}
		} else {
			fmt.Printf("  Did not generate explain plan because SQL is: %s\n", maxStep.SQL)
		}

		fmt.Printf("\n📊 Run %d: Processed %d records in %s\n", run, len(records), totalElapsed)
		fmt.Printf("⏱️ Per-record latency:\n")
		fmt.Printf("  - p50: %s\n", p50)
		fmt.Printf("  - p90: %s\n", p90)
		fmt.Printf("  - p99: %s\n", p99)
		fmt.Printf("  - maxTime: %s (%s)\n", maxTime, maxStep.Label)
		fmt.Printf("  - maxStep Explain:%s\n", maxStep.Explain)

		WriteCSVWithExplain(
			totalElapsed, p50, p90, p99, maxTime, len(records), maxStep, outputCSVPath, startFreshCSVFile,
		)
	}
}

func runInstrumentedTransaction(db *gorm.DB, rec benchmark.InputRecord) ([]benchmark.StepTiming, error) {
	var timings []benchmark.StepTiming
	var innerErr error

	err := db.Transaction(func(tx *gorm.DB) error {
		timingsResult, err := benchmark.ProcessRecordOption1Instrumented(tx, rec)
		if err != nil {
			innerErr = err
			return err
		}
		timings = timingsResult
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})

	if err != nil {
		return timings, err
	}
	return timings, innerErr
}

func WriteCSVWithExplain(total time.Duration, p50, p90, p99, max time.Duration, count int, maxStep benchmark.StepTiming, filePath string, writeHeaders bool) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("❌ Failed to open CSV: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeaders {
		writer.Write([]string{"Timestamp", "TotalTime", "P50", "P90", "P99", "MaxTime", "RecordCount", "MaxStepLabel", "MaxStepSQL", "MaxStepExplainPlan"})
	}

	record := []string{
		time.Now().Format("2006-01-02 15:04:05"),
		total.String(),
		p50.String(),
		p90.String(),
		p99.String(),
		max.String(),
		fmt.Sprintf("%d", count),
		maxStep.Label,
		maxStep.SQL,
		maxStep.Explain,
	}

	writer.Write(record)
}
