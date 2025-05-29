package benchmark

import (
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

func DenormalizedReferences2RepTablesBatched(b *testing.B) {

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

func ProcessRecord(b *testing.B, db *gorm.DB, rec InputRecord) {
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
					LocalResourceID: resourceID.String(),
					ReporterType:    "inventory",
					ResourceType:    rec.ResourceType,
					Version:         1,
					Data:            commonData,
				},
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
					LocalResourceID: rec.LocalResourceID,
					ReporterType:    rec.ReporterType,
					ResourceType:    rec.ResourceType,
					Version:         1,
					Data:            reporterData,
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
						fmt.Println("version for common rep update", ref.RepresentationVersion)
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
	}, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		b.Errorf("transaction failed: %v", err)
	}
}

// add indexes, unique
