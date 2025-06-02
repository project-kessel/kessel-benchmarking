package benchmark

import (
	"bufio"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yourusername/go-db-bench/config"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func RunTestForOption(t *testing.T, option func(*gorm.DB, InputRecord) ([]StepTiming, error), runCount int, inputRecordsPath string, outputPerRecordCSVPath string, outputCSVPath string) {
	startFreshCSVFile := true
	cfg := config.LoadDBConfig()

	for run := 1; run <= runCount; run++ {
		if run > 1 {
			startFreshCSVFile = false
		}
		fmt.Printf("\n🔁 Starting run %d/%d\n", run, runCount)

		// 1. Load input records from file
		records, err := LoadInputRecords("/Users/snehagunta/git/kessel/kessel-benchmarking/benchmark/input_files/" + inputRecordsPath)
		if err != nil {
			t.Fatalf("failed to load input records: %v", err)
		}

		//2. Timed: Execute the transaction, capture times for processing per record, times per SQL stmt, total time to process all records
		durations := make([]time.Duration, 0, len(records))
		allStepTimings := [][]StepTiming{}
		totalElapsed, durations, allStepTimings, err := ExecuteRun(cfg, t, records, durations, allStepTimings, option)
		if err != nil {
			return
		}

		//3. Write all record level outputs to csv
		err = WriteCSVAllRecords(run, durations, allStepTimings, outputPerRecordCSVPath)
		if err != nil {
			t.Fatalf("failed to write CSV for all records: %v", err)
		}

		//4. Analyze the run
		p50, p90, p99, maxTime, maxStep := AnalyzeRun(durations, allStepTimings, run, records, totalElapsed)

		// write aggregated records to csv
		WriteCSVForRun(
			run, totalElapsed, p50, p90, p99, maxTime, len(records), maxStep, outputCSVPath, startFreshCSVFile,
		)

	}
}

func LoadInputRecords(path string) ([]InputRecord, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {

		}
	}(file)

	var records []InputRecord
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var rec InputRecord
		if err := json.Unmarshal(scanner.Bytes(), &rec); err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, scanner.Err()
}

type InputRecord struct {
	ResourceType       string          `json:"resource_type"`
	ReporterType       string          `json:"reporter_type"`
	ReporterInstanceID string          `json:"reporter_instance_id"`
	LocalResourceID    string          `json:"local_resource_id"`
	APIHref            string          `json:"api_href"`
	ConsoleHref        string          `json:"console_href"`
	ReporterVersion    string          `json:"reporter_version"`
	Common             json.RawMessage `json:"common"`
	Reporter           json.RawMessage `json:"reporter"`
}

type StepTiming struct {
	Label    string
	SQL      string
	Duration time.Duration
	Explain  string
	Vars     []interface{}
}

func ProcessRecordOption3(tx *gorm.DB, rec InputRecord) error {
	var refs []models.RepresentationReference
	err := tx.
		Table("representation_references_option1 AS r1").
		Joins("JOIN representation_references_option1 AS r2 ON r1.resource_id = r2.resource_id").
		Where("r1.local_resource_id = ? AND r1.reporter_type = ? AND r1.resource_type = ? AND r1.reporter_instance_id = ?",
			rec.LocalResourceID, rec.ReporterType, rec.ResourceType, rec.ReporterInstanceID).
		Select("r2.*").
		Scan(&refs).Error
	if err != nil {
		return err
	}

	if len(refs) == 0 {
		// Create new resource
		resourceID := uuid.New()
		res := models.Resource{
			ID:   resourceID,
			Type: rec.ResourceType,
		}
		if err := tx.Create(&res).Error; err != nil {
			return err
		}

		// Insert common and reporter representation_references
		refsToCreate := []models.RepresentationReference{
			{
				ResourceID:            resourceID,
				LocalResourceID:       rec.LocalResourceID,
				ReporterType:          rec.ReporterType,
				ReporterInstanceID:    rec.ReporterInstanceID,
				ResourceType:          rec.ResourceType,
				RepresentationVersion: 1,
				Generation:            1,
				Tombstone:             false,
			},
			{
				ResourceID:            resourceID,
				LocalResourceID:       resourceID.String(),
				ReporterType:          "inventory",
				RepresentationVersion: 1,
				Generation:            1,
				Tombstone:             false,
			},
		}
		if err := tx.Create(&refsToCreate).Error; err != nil {
			return err
		}

		var commonData datatypes.JSON
		if rec.Common == nil || len(rec.Common) == 0 {
			defaultCommon := map[string]string{"workspaceId": "default"}
			bytes, err := json.Marshal(defaultCommon)
			if err != nil {
				return err
			}
			commonData = datatypes.JSON(bytes)
		} else {
			commonData = datatypes.JSON(rec.Common)
		}

		// Create representations
		if err := tx.Create(&models.CommonRepresentation{
			BaseRepresentation: models.BaseRepresentation{
				Data: commonData,
			},
			LocalResourceID: resourceID.String(),
			ReporterType:    "inventory",
			ResourceType:    rec.ResourceType,
			Version:         1,
		}).Error; err != nil {
			return err
		}

		var reporterData datatypes.JSON
		if rec.Reporter == nil || len(rec.Reporter) == 0 {
			reporterData = datatypes.JSON([]byte(`{}`))
		} else {
			reporterData = datatypes.JSON(rec.Reporter)
		}

		if err := tx.Create(&models.ReporterRepresentation{
			BaseRepresentation: models.BaseRepresentation{
				Data: reporterData,
			},
			LocalResourceID:    rec.LocalResourceID,
			ReporterType:       rec.ReporterType,
			ResourceType:       rec.ResourceType,
			Version:            1,
			ReporterVersion:    rec.ReporterVersion,
			ReporterInstanceID: rec.ReporterInstanceID,
			APIHref:            rec.APIHref,
			ConsoleHref:        rec.ConsoleHref,
			CommonVersion:      1,
			Tombstone:          false,
			Generation:         1,
		}).Error; err != nil {
			return err
		}
	} else {
		var commonVersion int
		var reporterVersion int

		for _, ref := range refs {
			if ref.ReporterType == "inventory" {
				commonVersion = ref.RepresentationVersion
			} else if ref.ReporterType == rec.ReporterType {
				reporterVersion = ref.RepresentationVersion
			}
		}
		// Update case: bump version and generation
		for _, ref := range refs {
			ref.RepresentationVersion++
			if ref.ReporterType == "inventory" {
				if rec.Common != nil {
					newCommonVersion := commonVersion + 1
					if err := tx.Create(&models.CommonRepresentation{
						BaseRepresentation: models.BaseRepresentation{

							Data: datatypes.JSON(rec.Common),
						},
						LocalResourceID: refs[0].ResourceID.String(),
						ReporterType:    "inventory",
						ResourceType:    rec.ResourceType,
						Version:         ref.RepresentationVersion,
					}).Error; err != nil {
						return err
					}

					if err := tx.Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ?", refs[0].ResourceID, "inventory").
						Update("representation_version", newCommonVersion).Error; err != nil {
						return err
					}
				}
			} else {
				if rec.Reporter != nil {
					newReporterVersion := reporterVersion + 1
					if err := tx.Create(&models.ReporterRepresentation{
						BaseRepresentation: models.BaseRepresentation{

							Data: datatypes.JSON(rec.Reporter),
						},
						LocalResourceID:    rec.LocalResourceID,
						ReporterType:       rec.ReporterType,
						ResourceType:       rec.ResourceType,
						Version:            ref.RepresentationVersion,
						ReporterVersion:    rec.ReporterVersion,
						ReporterInstanceID: rec.ReporterInstanceID,
						APIHref:            rec.APIHref,
						ConsoleHref:        rec.ConsoleHref,
						CommonVersion:      refs[0].RepresentationVersion,
						Tombstone:          false,
						Generation:         ref.Generation,
					}).Error; err != nil {
						return err
					}

					// Update representation_reference
					if err := tx.Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ? AND local_resource_id = ?", refs[0].ResourceID, rec.ReporterType, rec.LocalResourceID).
						Update("representation_version", newReporterVersion).Error; err != nil {
						return err
					}
				}
			}
		}
	}

	return nil
}

func GetExplainPlan(tx *gorm.DB, sql string, vars []interface{}) string {
	//explainSQL := "EXPLAIN (ANALYZE, BUFFERS) " + sql
	explainSQL := "EXPLAIN " + sql
	rows, err := tx.Raw(explainSQL, vars...).Rows()
	if err != nil {
		return fmt.Sprintf("❌ failed to get explain: %v", err)
	}
	defer rows.Close()

	var output []string
	for rows.Next() {
		var line string
		if err := rows.Scan(&line); err == nil {
			output = append(output, line)
		}
	}
	explainPlan := strings.Join(output, "\n")
	//fmt.Printf("Explain plan calculated: %s\n", explainPlan)
	return explainPlan
}

func WriteCSVAllRecords(
	run int,
	durations []time.Duration,
	allStepTimings [][]StepTiming,
	outputPath string,
) error {
	// Check if file exists and is empty
	writeHeader := false
	fileInfo, err := os.Stat(outputPath)
	if os.IsNotExist(err) {
		writeHeader = true
	} else if err != nil {
		return fmt.Errorf("failed to stat file: %w", err)
	} else if fileInfo.Size() == 0 {
		writeHeader = true
	}

	var file *os.File
	if writeHeader {
		file, err = os.Create(outputPath)
	} else {
		file, err = os.OpenFile(outputPath, os.O_APPEND|os.O_WRONLY, 0644)
	}
	if err != nil {
		return fmt.Errorf("failed to open CSV file: %w", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeader {
		header := []string{"run", "record_index", "record_duration_ms", "step_label", "step_duration_ms", "sql", "vars", "explain"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write header: %w", err)
		}
	}

	for i, stepTimings := range allStepTimings {
		duration := durations[i]
		for _, step := range stepTimings {
			row := []string{
				strconv.Itoa(run),
				strconv.Itoa(i),
				fmt.Sprintf("%.3f", duration.Seconds()*1000),
				step.Label,
				fmt.Sprintf("%.3f", step.Duration.Seconds()*1000),
				step.SQL,
				fmt.Sprintf("%v", step.Vars),
				step.Explain,
			}
			if err := writer.Write(row); err != nil {
				return fmt.Errorf("failed to write row: %w", err)
			}
		}
	}

	return nil
}

func AnalyzeRun(durations []time.Duration, allStepTimings [][]StepTiming, run int, records []InputRecord, totalElapsed time.Duration) (time.Duration, time.Duration, time.Duration, time.Duration, StepTiming) {
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

	var maxStep StepTiming
	for _, step := range maxTimings {
		if step.Duration > maxStep.Duration {
			maxStep = step
		}
	}

	fmt.Printf("\n📊 Run %d: Processed %d records in %s\n", run, len(records), totalElapsed)
	fmt.Printf("⏱️ Per-record latency:\n")
	fmt.Printf("  - p50: %s\n", p50)
	fmt.Printf("  - p90: %s\n", p90)
	fmt.Printf("  - p99: %s\n", p99)
	fmt.Printf("  - maxTime: %s (%s)\n", maxTime, maxStep.Label)
	//fmt.Printf("  - maxStep Explain:%s\n", maxStep.Explain)
	return p50, p90, p99, maxTime, maxStep
}

func ExecuteRun(cfg config.DBConfig, t *testing.T, records []InputRecord, durations []time.Duration, allStepTimings [][]StepTiming, transaction func(*gorm.DB, InputRecord) ([]StepTiming, error)) (time.Duration, []time.Duration, [][]StepTiming, error) {
	if err := config.DropAndRecreateDatabase(cfg); err != nil {
		t.Fatalf("❌ failed to reset DB: %v", err)
	}

	db := config.ConnectDB()
	sqlDB, err := db.DB()

	defer func(sqlDB *sql.DB) {
		err := sqlDB.Close()
		if err != nil {

		}
	}(sqlDB)
	// Reinitialize extensions if needed (like uuid-ossp or pgcrypto)
	// db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")

	// GORM auto-migration to recreate tables
	err = db.AutoMigrate(
		&models.Resource{},
		&models.CommonRepresentation{},
		&models.ReporterRepresentation{},
		&models.RepresentationReference{},
	)

	if err != nil {
		return 0, nil, nil, err
	}

	startTotal := time.Now()
	for i, rec := range records {
		start := time.Now()
		stepTimings, err := runInstrumentedTransaction(db, rec, transaction)
		duration := time.Since(start)

		durations = append(durations, duration)
		allStepTimings = append(allStepTimings, stepTimings)

		if err != nil {
			t.Errorf("record %d transaction failed: %v", i, err)
		}
	}
	totalElapsed := time.Since(startTotal)
	return totalElapsed, durations, allStepTimings, err
}

func runInstrumentedTransaction(db *gorm.DB, rec InputRecord, transaction func(*gorm.DB, InputRecord) ([]StepTiming, error)) ([]StepTiming, error) {
	var timings []StepTiming
	var innerErr error

	err := db.Transaction(func(tx *gorm.DB) error {
		timingsResult, err := transaction(tx, rec)
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

func WriteCSVForRun(run int, total time.Duration, p50, p90, p99, max time.Duration, count int, maxStep StepTiming, filePath string, writeHeaders bool) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("❌ Failed to open CSV: %v\n", err)
		return
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	if writeHeaders {
		writer.Write([]string{"Run no", "Timestamp", "TotalTime", "P50", "P90", "P99", "MaxTime", "RecordCount", "MaxStepLabel", "MaxStepSQL", "MaxStepExplainPlan"})
	}

	record := []string{
		fmt.Sprintf("%d", run),
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

	err = writer.Write(record)
	if err != nil {
		return
	}
}

func openCSVWithDateSuffix(filePath string) (*os.File, error) {
	// Extract base name and extension
	ext := filepath.Ext(filePath)
	base := strings.TrimSuffix(filepath.Base(filePath), ext)
	dir := filepath.Dir(filePath)

	// Append today's date
	dateSuffix := time.Now().Format("2006-01-02")
	fileName := fmt.Sprintf("%s_%s%s", base, dateSuffix, ext)
	fullPath := filepath.Join(dir, fileName)

	// Open or create file in append mode
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("❌ Failed to open or create CSV file: %w", err)
	}
	return file, nil
}
