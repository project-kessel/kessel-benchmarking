package regular_tests

import (
	"encoding/json"
	"github.com/google/uuid"
	"github.com/yourusername/go-db-bench/db/schemas/option1_denormalized_reference_2_rep_tables/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"testing"
	"time"

	"github.com/yourusername/go-db-bench/benchmark"
)

var runCount = 5

const inputRecordsPath = "input_10_records.jsonl"
const outputCSVPath = "per_run_results_option1_10.csv"
const outputPerRecordCSVPath = "per_record_results_option1_10.csv"
const generateExplainPlan = false

func TestDenormalizedRefs2RepTables(t *testing.T) {
	if generateExplainPlan {

	} else {
		benchmark.RunTestForOption(t, ProcessRecordOption1Instrumented, runCount, inputRecordsPath, outputPerRecordCSVPath, outputCSVPath)
	}

}

func ProcessRecordOption1Instrumented(tx *gorm.DB, rec benchmark.InputRecord) ([]benchmark.StepTiming, error) {
	timings := []benchmark.StepTiming{}

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

	timings = append(timings, benchmark.StepTiming{
		Label:    "select_refs_join",
		SQL:      sql,
		Duration: time.Since(t0),
		Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
		timings = append(timings, benchmark.StepTiming{
			Label:    "insert_resource",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
		timings = append(timings, benchmark.StepTiming{
			Label:    "insert_refs",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
				Data: commonData,
			},
			LocalResourceID: resourceID.String(), ReporterType: "inventory",
			ResourceType: rec.ResourceType, Version: 1,
		}

		dry = tx.Session(&gorm.Session{DryRun: true}).Create(commonRep)
		sql = dry.Statement.SQL.String()
		vars = dry.Statement.Vars
		//fmt.Printf("  vars: %s\n", len(vars))
		//fmt.Printf("  sql: %s\n", sql)
		t0 = time.Now()
		err = tx.Create(commonRep).Error
		timings = append(timings, benchmark.StepTiming{
			Label:    "insert_common_rep",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
				Data: reporterData,
			},
			LocalResourceID: rec.LocalResourceID, ReporterType: rec.ReporterType,
			ResourceType: rec.ResourceType, Version: 1,
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
		timings = append(timings, benchmark.StepTiming{
			Label:    "insert_reporter_rep",
			SQL:      sql,
			Duration: time.Since(t0),
			Vars:     vars,
			Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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

							Data: datatypes.JSON(rec.Common),
						},
						LocalResourceID: refs[0].ResourceID.String(),
						ReporterType:    "inventory",
						ResourceType:    rec.ResourceType,
						Version:         ref.RepresentationVersion,
					}
					dry := tx.Session(&gorm.Session{DryRun: true}).Create(commonRep)
					sql = dry.Statement.SQL.String()
					vars = dry.Statement.Vars
					//fmt.Printf("  vars: %s\n", len(vars))
					//fmt.Printf("  sql: %s\n", sql)
					t0 = time.Now()
					err = tx.Create(commonRep).Error
					timings = append(timings, benchmark.StepTiming{
						Label:    "insert_common_rep",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
					timings = append(timings, benchmark.StepTiming{
						Label:    "update_common_rep_ref",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
					}
					dry := tx.Session(&gorm.Session{DryRun: true}).Create(reporterRep)
					sql = dry.Statement.SQL.String()
					vars = dry.Statement.Vars
					//fmt.Printf("  vars: %s\n", len(vars))
					//fmt.Printf("  sql: %s\n", sql)
					t0 = time.Now()
					err = tx.Create(reporterRep).Error

					timings = append(timings, benchmark.StepTiming{
						Label:    "insert_reporter_rep",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
					timings = append(timings, benchmark.StepTiming{
						Label:    "update_reporter_rep_ref",
						SQL:      sql,
						Duration: time.Since(t0),
						Vars:     vars,
						Explain:  benchmark.GetExplainPlan(tx, sql, vars),
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
