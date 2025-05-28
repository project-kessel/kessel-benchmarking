package benchmark

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yourusername/go-db-bench/config"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
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

	records, err := loadInputRecords("benchmark_input/input.jsonl")
	if err != nil {
		b.Fatalf("failed to load input records: %v", err)
	}

	for b.Loop() {
		for _, rec := range records {
			err := db.Transaction(func(tx *gorm.DB) error {

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

					// Create representations
					if err := tx.Create(&models.CommonRepresentation{
						BaseRepresentation: models.BaseRepresentation{
							LocalResourceID: resourceID.String(),
							ReporterType:    "inventory",
							ResourceType:    rec.ResourceType,
							Version:         1,
							Data:            datatypes.JSON(rec.Common),
						},
					}).Error; err != nil {
						return err
					}

					if err := tx.Create(&models.ReporterRepresentation{
						BaseRepresentation: models.BaseRepresentation{
							LocalResourceID: rec.LocalResourceID,
							ReporterType:    rec.ReporterType,
							ResourceType:    rec.ResourceType,
							Version:         1,
							Data:            datatypes.JSON(rec.Reporter),
						},
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
					// Update case: bump version and generation
					for _, ref := range refs {
						ref.RepresentationVersion++
						if ref.ReporterType == "inventory" {
							if err := tx.Create(&models.CommonRepresentation{
								BaseRepresentation: models.BaseRepresentation{
									LocalResourceID: refs[0].ResourceID.String(),
									ReporterType:    "inventory",
									ResourceType:    rec.ResourceType,
									Version:         ref.RepresentationVersion,
									Data:            datatypes.JSON(rec.Common),
								},
							}).Error; err != nil {
								return err
							}
						} else {
							if err := tx.Create(&models.ReporterRepresentation{
								BaseRepresentation: models.BaseRepresentation{
									LocalResourceID: rec.LocalResourceID,
									ReporterType:    rec.ReporterType,
									ResourceType:    rec.ResourceType,
									Version:         ref.RepresentationVersion,
									Data:            datatypes.JSON(rec.Reporter),
								},
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
						}
					}
				}

				return nil
			}, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				b.Errorf("transaction failed: %v", err)
			}
		}
	}
}

// add indexes, unique
