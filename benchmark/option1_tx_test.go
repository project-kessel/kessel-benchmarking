package benchmark

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/google/uuid"
	"gorm.io/datatypes"
	"gorm.io/gorm"

	"github.com/yourusername/go-db-bench/config"
	"github.com/yourusername/go-db-bench/db/schemas/denormalized_reference_2_rep_tables/models"
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

	for i := 0; i < b.N; i++ {
		for _, rec := range records {
			err := db.Transaction(func(tx *gorm.DB) error {

				var refs []models.RepresentationReference
				err := tx.
					Table("representation_references_option1").
					Joins("JOIN resources_option1 ON resources_option1.id = representation_references_option1.resource_id").
					Where("representation_references_option1.local_resource_id = ? AND representation_references_option1.reporter_instance_id = ? AND representation_references_option1.reporter_type = ? AND representation_references_option1.resource_type = ?",
						rec.LocalResourceID, rec.ReporterInstanceID, rec.ReporterType, rec.ResourceType).
					Find(&refs).Error
				if err != nil {
					return err
				}

				resourceID := uuid.New()

				if len(refs) == 0 {
					// Create new resource
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
						ref.Generation++
						if err := tx.Save(&ref).Error; err != nil {
							return err
						}
					}

					if err := tx.Create(&models.CommonRepresentation{
						BaseRepresentation: models.BaseRepresentation{
							LocalResourceID: resourceID.String(),
							ReporterType:    "inventory",
							ResourceType:    rec.ResourceType,
							Version:         refs[0].RepresentationVersion,
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
							Version:         refs[0].RepresentationVersion,
							Data:            datatypes.JSON(rec.Reporter),
						},
						ReporterVersion:    rec.ReporterVersion,
						ReporterInstanceID: rec.ReporterInstanceID,
						APIHref:            rec.APIHref,
						ConsoleHref:        rec.ConsoleHref,
						CommonVersion:      refs[0].RepresentationVersion,
						Tombstone:          false,
						Generation:         refs[0].Generation,
					}).Error; err != nil {
						return err
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
