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
	"strings"
	"time"
)

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

type StepTiming struct {
	Label    string
	SQL      string
	Duration time.Duration
	Explain  string
	Vars     []interface{}
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

func ProcessRecordOption1Instrumented(tx *gorm.DB, rec InputRecord) ([]StepTiming, error) {
	timings := []StepTiming{}

	query := tx.Session(&gorm.Session{DryRun: true}).Table("representation_references_option1 AS r1").
		Joins("JOIN representation_references_option1 AS r2 ON r1.resource_id = r2.resource_id").
		Where("r1.local_resource_id = ? AND r1.reporter_type = ? AND r1.resource_type = ? AND r1.reporter_instance_id = ?",
			rec.LocalResourceID, rec.ReporterType, rec.ResourceType, rec.ReporterInstanceID).
		Select("r2.*")
	_ = query.Find(&[]models.RepresentationReference{})

	sql := query.Statement.SQL.String()
	vars := query.Statement.Vars

	var refs []models.RepresentationReference
	t0 := time.Now()
	err := tx.Table("representation_references_option1 AS r1").
		Joins("JOIN representation_references_option1 AS r2 ON r1.resource_id = r2.resource_id").
		Where("r1.local_resource_id = ? AND r1.reporter_type = ? AND r1.resource_type = ? AND r1.reporter_instance_id = ?",
			rec.LocalResourceID, rec.ReporterType, rec.ResourceType, rec.ReporterInstanceID).
		Select("r2.*").
		Scan(&refs).Error

	timings = append(timings, StepTiming{
		Label:    "select_refs_join",
		SQL:      sql,
		Duration: time.Since(t0),
		Explain:  GetExplainPlan(tx, sql, vars),
		Vars:     vars,
	})
	if err != nil {
		return timings, err
	}

	if len(refs) == 0 {

		resourceID := uuid.New()
		res := models.Resource{
			ID:   resourceID,
			Type: rec.ResourceType,
		}
		dry := tx.Session(&gorm.Session{DryRun: true}).Create(&res)
		vars = dry.Statement.Vars
		sql = dry.Statement.SQL.String()
		//fmt.Printf("  vars: %s\n", len(vars))
		//fmt.Printf("  sql: %s\n", sql)
		t0 = time.Now()
		err = tx.Create(&res).Error
		timings = append(timings, StepTiming{
			Label:    "insert_resource",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  GetExplainPlan(tx, sql, vars),
		})
		if err != nil {
			return timings, err
		}

		refsToCreate := []models.RepresentationReference{
			{ResourceID: resourceID, LocalResourceID: rec.LocalResourceID, ReporterType: rec.ReporterType,
				ReporterInstanceID: rec.ReporterInstanceID, ResourceType: rec.ResourceType, RepresentationVersion: 1,
				Generation: 1, Tombstone: false},
			{ResourceID: resourceID, LocalResourceID: resourceID.String(), ReporterType: "inventory",
				RepresentationVersion: 1, Generation: 1, Tombstone: false},
		}

		dry = tx.Session(&gorm.Session{DryRun: true}).Create(&refsToCreate)
		sql = dry.Statement.SQL.String()
		vars = dry.Statement.Vars
		//fmt.Printf("  vars: %s\n", len(vars))
		//fmt.Printf("  sql: %s\n", sql)
		t0 = time.Now()
		err = tx.Create(&refsToCreate).Error
		timings = append(timings, StepTiming{
			Label:    "insert_refs",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  GetExplainPlan(tx, sql, vars),
		})
		if err != nil {
			return timings, err
		}

		var commonData datatypes.JSON
		if rec.Common == nil || len(rec.Common) == 0 {
			defaultCommon := map[string]string{"workspaceId": "default"}
			bytes, _ := json.Marshal(defaultCommon)
			commonData = bytes
		} else {
			commonData = datatypes.JSON(rec.Common)
		}

		commonRep := &models.CommonRepresentation{
			BaseRepresentation: models.BaseRepresentation{
				LocalResourceID: resourceID.String(), ReporterType: "inventory",
				ResourceType: rec.ResourceType, Version: 1, Data: commonData,
			},
		}

		dry = tx.Session(&gorm.Session{DryRun: true}).Create(commonRep)
		sql = dry.Statement.SQL.String()
		vars = dry.Statement.Vars
		//fmt.Printf("  vars: %s\n", len(vars))
		//fmt.Printf("  sql: %s\n", sql)
		t0 = time.Now()
		err = tx.Create(commonRep).Error
		timings = append(timings, StepTiming{
			Label:    "insert_common_rep",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  GetExplainPlan(tx, sql, vars),
		})
		if err != nil {
			return timings, err
		}

		var reporterData datatypes.JSON
		if rec.Reporter == nil || len(rec.Reporter) == 0 {
			reporterData = []byte(`{}`)
		} else {
			reporterData = datatypes.JSON(rec.Reporter)
		}

		reporterRep := &models.ReporterRepresentation{
			BaseRepresentation: models.BaseRepresentation{
				LocalResourceID: rec.LocalResourceID, ReporterType: rec.ReporterType,
				ResourceType: rec.ResourceType, Version: 1, Data: reporterData,
			},
			ReporterVersion: rec.ReporterVersion, ReporterInstanceID: rec.ReporterInstanceID,
			APIHref: rec.APIHref, ConsoleHref: rec.ConsoleHref, CommonVersion: 1,
			Tombstone: false, Generation: 1,
		}
		t0 = time.Now()
		dry = tx.Session(&gorm.Session{DryRun: true}).Create(reporterRep)
		sql = dry.Statement.SQL.String()
		vars = dry.Statement.Vars
		//fmt.Printf("  vars: %s\n", len(vars))
		//fmt.Printf("  sql: %s\n", sql)
		err = tx.Create(reporterRep).Error
		timings = append(timings, StepTiming{
			Label:    "insert_reporter_rep",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  GetExplainPlan(tx, sql, vars),
		})
		if err != nil {
			return timings, err
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
					commonRep := &models.CommonRepresentation{
						BaseRepresentation: models.BaseRepresentation{
							LocalResourceID: refs[0].ResourceID.String(),
							ReporterType:    "inventory",
							ResourceType:    rec.ResourceType,
							Version:         ref.RepresentationVersion,
							Data:            datatypes.JSON(rec.Common),
						},
					}
					dry := tx.Session(&gorm.Session{DryRun: true}).Create(commonRep)
					sql = dry.Statement.SQL.String()
					vars = dry.Statement.Vars
					//fmt.Printf("  vars: %s\n", len(vars))
					//fmt.Printf("  sql: %s\n", sql)
					t0 = time.Now()
					err = tx.Create(commonRep).Error
					timings = append(timings, StepTiming{
						Label:    "insert_common_rep",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  GetExplainPlan(tx, sql, vars),
					})
					if err != nil {
						return timings, err
					}

					updateRepRef := tx.Session(&gorm.Session{DryRun: true}).Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ?", refs[0].ResourceID, "inventory").
						Update("representation_version", newCommonVersion)
					sql = updateRepRef.Statement.SQL.String()
					vars = updateRepRef.Statement.Vars
					t0 = time.Now()
					err = tx.Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ?", refs[0].ResourceID, "inventory").
						Update("representation_version", newCommonVersion).Error
					timings = append(timings, StepTiming{
						Label:    "update_common_rep_ref",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  GetExplainPlan(tx, sql, vars),
					})
					if err != nil {
						return timings, err
					}
				}
			} else {
				if rec.Reporter != nil {
					newReporterVersion := reporterVersion + 1
					reporterRep := &models.ReporterRepresentation{
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
					}
					dry := tx.Session(&gorm.Session{DryRun: true}).Create(reporterRep)
					sql = dry.Statement.SQL.String()
					vars = dry.Statement.Vars
					//fmt.Printf("  vars: %s\n", len(vars))
					//fmt.Printf("  sql: %s\n", sql)
					t0 = time.Now()
					err = tx.Create(reporterRep).Error

					timings = append(timings, StepTiming{
						Label:    "insert_reporter_rep",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  GetExplainPlan(tx, sql, vars),
					})
					if err != nil {
						return timings, err
					}

					// Update representation_reference

					updateRepRef := tx.Session(&gorm.Session{DryRun: true}).Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ? AND local_resource_id = ?", refs[0].ResourceID, rec.ReporterType, rec.LocalResourceID).
						Update("representation_version", newReporterVersion)
					sql = updateRepRef.Statement.SQL.String()
					vars = updateRepRef.Statement.Vars
					t0 = time.Now()
					err = tx.Model(&models.RepresentationReference{}).
						Where("resource_id = ? AND reporter_type = ? AND local_resource_id = ?", refs[0].ResourceID, rec.ReporterType, rec.LocalResourceID).
						Update("representation_version", newReporterVersion).Error
					timings = append(timings, StepTiming{
						Label:    "update_reporter_rep_ref",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  GetExplainPlan(tx, sql, vars),
					})
					if err != nil {
						return timings, err
					}
				}
			}
		}
	}

	return timings, nil
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
