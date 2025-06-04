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

var runCount = 10

const inputRecordsPath = "input_100000_records.jsonl"
const outputCSVPath = "per_run_results_option1_100000_without_search_index.csv"
const outputPerRecordCSVPath = "per_record_results_option1_100000_without_search_index.csv"
const explain = false

func TestDenormalizedRefs2RepTables(t *testing.T) {
	benchmark.RunTestForOption(t, ProcessRecordOption1Instrumented, runCount, inputRecordsPath, outputPerRecordCSVPath, outputCSVPath)
}

func ProcessRecordOption1Instrumented(tx *gorm.DB, rec benchmark.InputRecord) ([]benchmark.StepTiming, error) {
	timings := []benchmark.StepTiming{}
	var results interface{}

	if explain {
		dryRunAndRecordExplainPlan(tx, timings, func() *gorm.DB {
			query, _ := buildSelectRefsQueryOption1(tx, rec, false)
			return query
		}, "select_refs_join")
	} else {
		var err error
		timings, err, results = actualRunAndRecordExecutionTiming(tx, timings,
			func() (*gorm.DB, interface{}) {
				query, results := buildSelectRefsQueryOption1(tx, rec, false)
				return query, results
			},
			"select_refs_join",
		)
		if err != nil {
			return timings, err
		}
	}
	refs := results.([]models.RepresentationReference)

	if len(refs) == 0 {

		resourceID := uuid.New()
		res := models.Resource{
			ID:   resourceID,
			Type: rec.ResourceType,
		}

		if explain {
			timings = dryRunAndRecordExplainPlan(tx, timings,
				func() *gorm.DB { return insertResource(tx, res, true) },
				"insert_resource",
			)
		} else {
			var err error
			timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
				func() (*gorm.DB, interface{}) { return insertResource(tx, res, false), nil },
				"insert_resource",
			)
			if err != nil {
				return timings, err
			}
		}

		refsToCreate := []models.RepresentationReference{
			{ResourceID: resourceID, LocalResourceID: rec.LocalResourceID, ReporterType: rec.ReporterType,
				ReporterInstanceID: rec.ReporterInstanceID, ResourceType: rec.ResourceType, RepresentationVersion: 1,
				Generation: 1, Tombstone: false},
			{ResourceID: resourceID, LocalResourceID: resourceID.String(), ReporterType: "inventory",
				RepresentationVersion: 1, Generation: 1, Tombstone: false},
		}

		if explain {
			timings = dryRunAndRecordExplainPlan(tx, timings,
				func() *gorm.DB { return insertRepresentationReferences(tx, refsToCreate, true) },
				"insert_refs",
			)
		} else {
			var err error
			timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
				func() (*gorm.DB, interface{}) { return insertRepresentationReferences(tx, refsToCreate, false), nil },
				"insert_refs",
			)
			if err != nil {
				return timings, err
			}
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

		if explain {
			timings = dryRunAndRecordExplainPlan(tx, timings,
				func() *gorm.DB { return insertCommonRepresentation(tx, commonRep, true) },
				"insert_common_rep",
			)
		} else {
			var err error
			timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
				func() (*gorm.DB, interface{}) { return insertCommonRepresentation(tx, commonRep, false), nil },
				"insert_common_rep",
			)
			if err != nil {
				return timings, err
			}
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
		if explain {
			timings = dryRunAndRecordExplainPlan(tx, timings,
				func() *gorm.DB { return insertReporterRepresentation(tx, reporterRep, true) },
				"insert_reporter_rep",
			)
		} else {
			var err error
			timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
				func() (*gorm.DB, interface{}) { return insertReporterRepresentation(tx, reporterRep, false), nil },
				"insert_reporter_rep",
			)
			if err != nil {
				return timings, err
			}
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
						Version:         newCommonVersion,
					}
					if explain {
						timings = dryRunAndRecordExplainPlan(tx, timings,
							func() *gorm.DB { return insertCommonRepresentation(tx, commonRep, true) },
							"insert_common_rep",
						)
					} else {
						var err error
						timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
							func() (*gorm.DB, interface{}) { return insertCommonRepresentation(tx, commonRep, false), nil },
							"insert_common_rep",
						)
						if err != nil {
							return timings, err
						}
					}

					if explain {
						timings = dryRunAndRecordExplainPlan(
							tx,
							timings,
							func() *gorm.DB {
								return updateCommonRepresentationVersion(tx, refs[0].ResourceID, newCommonVersion, true)
							},
							"update_reporter_rep_ref")
					} else {
						var err error
						timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
							func() (*gorm.DB, interface{}) {
								return updateCommonRepresentationVersion(tx, refs[0].ResourceID, newCommonVersion, false), nil
							},
							"insert_reporter_rep",
						)
						if err != nil {
							return timings, err
						}
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
						Version:            newReporterVersion,
						ReporterVersion:    rec.ReporterVersion,
						ReporterInstanceID: rec.ReporterInstanceID,
						APIHref:            rec.APIHref,
						ConsoleHref:        rec.ConsoleHref,
						CommonVersion:      refs[0].RepresentationVersion,
						Tombstone:          false,
						Generation:         ref.Generation,
					}
					if explain {
						timings = dryRunAndRecordExplainPlan(tx, timings,
							func() *gorm.DB { return insertReporterRepresentation(tx, reporterRep, true) },
							"insert_reporter_rep",
						)
					} else {
						var err error
						timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
							func() (*gorm.DB, interface{}) { return insertReporterRepresentation(tx, reporterRep, false), nil },
							"insert_reporter_rep",
						)
						if err != nil {
							return timings, err
						}
					}

					// Update representation_reference
					if explain {
						timings = dryRunAndRecordExplainPlan(
							tx,
							timings,
							func() *gorm.DB {
								return updateReporterRepresentationVersion(tx, refs[0].ResourceID, rec.ReporterType, rec.LocalResourceID, newReporterVersion, true)
							},
							"update_reporter_rep_ref")
					} else {
						var err error
						timings, err, _ = actualRunAndRecordExecutionTiming(tx, timings,
							func() (*gorm.DB, interface{}) {
								query := updateReporterRepresentationVersion(tx, refs[0].ResourceID, rec.ReporterType, rec.LocalResourceID, newReporterVersion, false)
								return query, nil
							},
							"insert_reporter_rep",
						)
						if err != nil {
							return timings, err
						}
					}
				}
			}
		}
	}

	return timings, nil
}

func updateCommonRepresentationVersion(
	tx *gorm.DB,
	resourceID uuid.UUID,
	newVersion int,
	dryRun bool,
) *gorm.DB {
	session := tx.Session(&gorm.Session{DryRun: dryRun})
	query := session.Model(&models.RepresentationReference{}).
		Where("resource_id = ? AND reporter_type = ?", resourceID, "inventory").
		Update("representation_version", newVersion)
	return query
}

func updateReporterRepresentationVersion(
	tx *gorm.DB,
	resourceID uuid.UUID,
	reporterType string,
	localResourceID string,
	newVersion int,
	dryRun bool,
) *gorm.DB {
	//fmt.Printf("Reporter Rep to update: %d\n", newVersion)
	session := tx.Session(&gorm.Session{DryRun: dryRun})
	query := session.Model(&models.RepresentationReference{}).
		Where("resource_id = ? AND reporter_type = ? AND local_resource_id = ?", resourceID, reporterType, localResourceID).
		Update("representation_version", newVersion)
	return query
}

func insertReporterRepresentation(tx *gorm.DB, reporterRep *models.ReporterRepresentation, dryRun bool) *gorm.DB {
	//fmt.Printf("Reporter Rep to Insert: %+v\n", reporterRep.Version)
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(reporterRep)
}

func insertCommonRepresentation(tx *gorm.DB, commonRep *models.CommonRepresentation, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(commonRep)
}

func buildSelectRefsQueryOption1(
	tx *gorm.DB,
	rec benchmark.InputRecord,
	dryRun bool,
) (*gorm.DB, []models.RepresentationReference) {
	var refs []models.RepresentationReference

	query := tx.Session(&gorm.Session{DryRun: dryRun}).
		Table("representation_references_option1 AS r1").
		Joins("JOIN representation_references_option1 AS r2 ON r1.resource_id = r2.resource_id").
		Where("r1.local_resource_id = ? AND r1.reporter_type = ? AND r1.resource_type = ? AND r1.reporter_instance_id = ?",
			rec.LocalResourceID, rec.ReporterType, rec.ResourceType, rec.ReporterInstanceID).
		Select("r2.*")

	if !dryRun {
		query = query.Scan(&refs)
	}

	return query, refs
}

func insertResource(tx *gorm.DB, resource models.Resource, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(&resource)
}

func insertRepresentationReferences(tx *gorm.DB, refsToCreate []models.RepresentationReference, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(&refsToCreate)
}

func dryRunAndRecordExplainPlan(
	tx *gorm.DB,
	timings []benchmark.StepTiming,
	dryFunc func() *gorm.DB,
	label string,
) []benchmark.StepTiming {
	dry := dryFunc()
	sql := dry.Statement.SQL.String()
	vars := dry.Statement.Vars

	timings = append(timings, benchmark.StepTiming{
		Label:   label,
		SQL:     sql,
		Vars:    vars,
		Explain: benchmark.GetExplainPlan(tx, sql, vars),
	})
	return timings
}

func actualRunAndRecordExecutionTiming(
	tx *gorm.DB,
	timings []benchmark.StepTiming,
	execFunc func() (*gorm.DB, interface{}),
	label string,
) ([]benchmark.StepTiming, error, interface{}) {
	t0 := time.Now()
	exec, result := execFunc()
	err := exec.Error
	timings = append(timings, benchmark.StepTiming{
		Label:    label,
		Duration: time.Since(t0),
	})
	return timings, err, result
}
