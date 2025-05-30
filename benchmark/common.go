package benchmark

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"os"
	"time"
)

func ResetDatabase(db *gorm.DB) error {

	err := ResetSchema(db)
	if err != nil {
		return err
	}

	err = ResetSessionConfig(db)
	if err != nil {
		return err
	}

	err = ConfirmNoTables(db)
	if err != nil {
		return err
	}

	err = Automigrate(db)
	if err != nil {
		return err
	}
	return err
}

func ResetSchema(db *gorm.DB) error {
	// Optional: Drop schema (PostgreSQL syntax)
	if err := db.Exec("DROP SCHEMA public CASCADE").Error; err != nil {
		return err
	}

	if err := db.Exec("CREATE SCHEMA public").Error; err != nil {
		return err
	}
	fmt.Println("✅ Dropped and recreated public schema")

	return nil
}

func Automigrate(db *gorm.DB) error {
	// Reinitialize extensions if needed (like uuid-ossp or pgcrypto)
	// db.Exec("CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\"")

	// GORM auto-migration to recreate tables
	err := db.AutoMigrate(
		&models.Resource{},
		&models.CommonRepresentation{},
		&models.ReporterRepresentation{},
		&models.RepresentationReference{},
	)
	return err
}

func ResetSessionConfig(db *gorm.DB) error {
	// Optional: reset planner/session settings
	err := db.Exec(`DISCARD ALL;`).Error
	if err != nil {
		return err
	}
	fmt.Println("✅ DISCARD ALL executed")
	err = db.Exec("RESET ALL").Error
	if err != nil {
		return err
	}

	fmt.Println("✅ RESET ALL executed")
	return err
}

func ConfirmNoTables(db *gorm.DB) error {
	// Confirm clean
	var tables []string
	if err := db.Raw(`SELECT tablename FROM pg_tables WHERE schemaname = 'public'`).Scan(&tables).Error; err != nil {
		return fmt.Errorf("failed to query tables: %w", err)
	}
	if len(tables) > 0 {
		return fmt.Errorf("❌ tables still exist after reset: %v", tables)
	}
	fmt.Println("✅ Verified: no user-defined tables remain")

	return nil
}

func WriteCSV(total time.Duration, p50, p90, p99, max time.Duration, count int, outputCSVPath string, startFreshCSVFile bool) {
	mode := os.O_APPEND | os.O_CREATE | os.O_WRONLY
	if startFreshCSVFile {
		mode = os.O_CREATE | os.O_WRONLY | os.O_TRUNC
	}

	f, err := os.OpenFile(outputCSVPath, mode, 0644)
	if err != nil {
		fmt.Printf("❌ Failed to write CSV: %v\n", err)
		return
	}
	defer f.Close()

	writer := csv.NewWriter(f)
	defer writer.Flush()

	if startFreshCSVFile {
		_ = writer.Write([]string{"Timestamp", "Records", "TotalTime", "p50", "p90", "p99", "Max"})
	}

	record := []string{
		time.Now().Format("2006-01-02 15:04:05"),
		fmt.Sprintf("%d", count),
		total.String(),
		p50.String(),
		p90.String(),
		p99.String(),
		max.String(),
	}

	_ = writer.Write(record)
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

func ProcessRecordOption1(tx *gorm.DB, rec InputRecord) error {
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
}
