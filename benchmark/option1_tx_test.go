package benchmark

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/yourusername/go-db-bench/config"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"os"
	"testing"
)

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

func loadInputRecords(path string) ([]InputRecord, error) {
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

func BenchmarkDenormalizedReferences2RepTables(b *testing.B) {

	db := config.ConnectDB()
	_ = db.AutoMigrate(
		&models.Resource{},
		&models.CommonRepresentation{},
		&models.ReporterRepresentation{},
		&models.RepresentationReference{},
	)

	wd, _ := os.Getwd()
	fmt.Println("Current working directory:", wd)

	records, err := loadInputRecords("benchmark_input/input_100_records.jsonl")
	if err != nil {
		b.Fatalf("failed to load input records: %v", err)
	}

	for b.Loop() {
		for _, rec := range records {
			ProcessRecord(b, db, rec)
		}
	}
}

// add indexes, unique
