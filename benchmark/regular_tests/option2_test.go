package regular_tests

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/yourusername/go-db-bench/benchmark"
	option2models "github.com/yourusername/go-db-bench/db/schemas/option2_normalized_reference_2_rep_tables/models"
	"gorm.io/datatypes"
	"gorm.io/gorm"
	"testing"
)

func TestNormalizedRefs2RepTables(t *testing.T) {
	benchmark.RunTestForOption(t, ProcessRecordOption2, runCount, inputRecordsPath, outputPerRecordCSVPath, outputCSVPath)
}

func ProcessRecordOption2(tx *gorm.DB, rec benchmark.InputRecord) ([]benchmark.StepTiming, error) {
	timings := []benchmark.StepTiming{}
	var results interface{}
	var err error

	if explain {
		benchmark.DryRunAndRecordExplainPlan(tx, timings, func() *gorm.DB {
			query, _ := buildSelectRefsQueryOption2(tx, rec, true)
			return query
		}, "select_refs_and_reps_join")
	} else {
		timings, err, results = benchmark.ActualRunAndRecordExecutionTiming(tx, timings,
			func() (*gorm.DB, interface{}) {
				query, results := buildSelectRefsQueryOption2(tx, rec, false)
				return query, results
			},
			"select_refs_and_reps_join",
		)
		if err != nil {
			return timings, err
		}
	}

	refs := results.([]option2models.JoinedRepresentation)

	if len(refs) == 0 {
		timings, err = CreateResourceAndRepresentationsOption2(tx, rec, timings, explain)
		if err != nil {
			return timings, err
		}
	} else {
		timings, err = updateResourceAndRepresentationsOption2(tx, timings, rec, refs, explain)
		if err != nil {
			return nil, err
		}
	}
	return timings, nil
}

func CreateResourceAndRepresentationsOption2(
	tx *gorm.DB,
	rec benchmark.InputRecord,
	timings []benchmark.StepTiming,
	explain bool,
) ([]benchmark.StepTiming, error) {
	//fmt.Println("Creating Resource and Representation")
	resourceID := uuid.New()
	res := option2models.Resource{ID: resourceID, Type: rec.ResourceType}

	//fmt.Printf("Inserting Resource")
	// Insert Resource
	timings, err, _ := conditionalInsert(
		tx, timings, "insert_resource", explain,
		func(dry bool) *gorm.DB { return insertResourceOption2(tx, res, dry) },
	)
	if err != nil {
		return timings, err
	}

	// Prepare CommonRepresentation
	commonRepresentationId := uuid.New()
	commonData := prepareJSON(rec.Common, map[string]string{"workspaceId": "default"})
	commonRep := &option2models.CommonRepresentation{
		ID:              commonRepresentationId,
		LocalResourceID: resourceID.String(),
		Version:         1,
		ResourceType:    rec.ResourceType,
		BaseRepresentation: option2models.BaseRepresentation{
			Data: commonData,
		},
		ReportedBy: rec.ReporterType,
	}

	timings, err, _ = conditionalInsert(
		tx, timings, "insert_common_rep",
		explain,
		func(dry bool) *gorm.DB { return insertCommonRepresentationOption2(tx, commonRep, dry) },
	)
	if err != nil {
		return timings, err
	}

	cv := 1
	// Prepare ReporterRepresentation
	reporterRepresentationId := uuid.New()
	reporterData := prepareJSON(rec.Reporter, map[string]string{})
	reporterRep := &option2models.ReporterRepresentation{
		ID:                 reporterRepresentationId,
		LocalResourceID:    rec.LocalResourceID,
		ReporterType:       rec.ReporterType,
		ResourceType:       rec.ResourceType,
		Version:            1,
		ReporterVersion:    rec.ReporterVersion,
		ReporterInstanceID: rec.ReporterInstanceID,
		APIHref:            rec.APIHref,
		ConsoleHref:        rec.ConsoleHref,
		CommonVersion:      &cv,
		Tombstone:          false,
		Generation:         1,
		BaseRepresentation: option2models.BaseRepresentation{
			Data: reporterData,
		},
	}

	timings, err, _ = conditionalInsert(
		tx, timings, "insert_reporter_rep",
		explain,
		func(dry bool) *gorm.DB { return insertReporterRepresentationOption2(tx, reporterRep, dry) },
	)
	if err != nil {
		return timings, err
	}

	// Insert representation_references for reporter_representation and common_representation
	timings, err, _ = conditionalInsert(
		tx, timings, "insert_rep_refs",
		explain,
		func(dry bool) *gorm.DB {
			refs := []option2models.RepresentationReference{
				{
					ReporterRepresentationID: &reporterRepresentationId,
					ResourceID:               resourceID,
					CommonRepresentationID:   nil,
				},
				{
					CommonRepresentationID:   &commonRepresentationId,
					ResourceID:               resourceID,
					ReporterRepresentationID: nil,
				},
			}
			return insertRepresentationReferencesOption2(tx, refs, dry)
		},
	)
	if err != nil {
		return timings, err
	}

	return timings, nil
}

func updateResourceAndRepresentationsOption2(
	tx *gorm.DB,
	timings []benchmark.StepTiming,
	rec benchmark.InputRecord,
	joinedReps []option2models.JoinedRepresentation,
	explain bool,
) ([]benchmark.StepTiming, error) {
	var (
		commonVersion, reporterVersion, generation int
		resourceID                                 = joinedReps[0].ResourceID
	)

	//fmt.Println("✅ Updating reps and refs", len(joinedReps))

	// Determine latest versions
	for _, joined := range joinedReps {
		if joined.Reporter.ReporterType == "" || joined.Reporter.ReporterType == "inventory" {
			commonVersion = joined.Common.Version
		} else if joined.Reporter.ReporterType == rec.ReporterType {
			reporterVersion = joined.Reporter.Version
			generation = joined.Reporter.Generation
		}
	}

	newReporterVersion := reporterVersion + 1
	newCommonVersion := commonVersion + 1
	newReporterID := uuid.New()
	newCommonID := uuid.New()

	shouldInsertCommon := rec.Common != nil && len(rec.Common) > 0
	shouldInsertReporter := rec.Reporter != nil && len(rec.Reporter) > 0

	var err error

	// Insert new CommonRepresentation
	if shouldInsertCommon {
		//fmt.Println("✅ Inserting new common rep")
		commonRep := &option2models.CommonRepresentation{
			ID:              newCommonID,
			LocalResourceID: resourceID.String(),
			Version:         newCommonVersion,
			ResourceType:    rec.ResourceType,
			ReportedBy:      rec.ReporterType,
			BaseRepresentation: option2models.BaseRepresentation{
				Data: datatypes.JSON(rec.Common),
			},
		}
		timings, err, _ = conditionalInsert(
			tx, timings, "insert_common_rep", explain,
			func(dry bool) *gorm.DB {
				return insertCommonRepresentationOption2(tx, commonRep, dry)
			},
		)
		if err != nil {
			return timings, err
		}
	}

	// Insert new ReporterRepresentation
	if shouldInsertReporter {
		//fmt.Println("✅ Inserting new reporter rep")
		var commonVersionPtr *int
		if shouldInsertCommon {
			commonVersionPtr = &newCommonVersion
		}

		reporterRep := &option2models.ReporterRepresentation{
			ID:                 newReporterID,
			LocalResourceID:    rec.LocalResourceID,
			ReporterType:       rec.ReporterType,
			ResourceType:       rec.ResourceType,
			Version:            newReporterVersion,
			ReporterVersion:    rec.ReporterVersion,
			ReporterInstanceID: rec.ReporterInstanceID,
			APIHref:            rec.APIHref,
			ConsoleHref:        rec.ConsoleHref,
			CommonVersion:      commonVersionPtr,
			Tombstone:          false,
			Generation:         generation,
			BaseRepresentation: option2models.BaseRepresentation{
				Data: datatypes.JSON(rec.Reporter),
			},
		}

		timings, err, _ = conditionalInsert(
			tx, timings, "insert_reporter_rep", explain,
			func(dry bool) *gorm.DB {
				return insertReporterRepresentationOption2(tx, reporterRep, dry)
			},
		)
		if err != nil {
			return timings, err
		}
	}

	// ✅ Update reference table rows individually
	if shouldInsertCommon {
		//fmt.Println("✅ Updating common rep")
		timings, err, _ = conditionalInsert(
			tx, timings, "update_ref_common", explain,
			func(dry bool) *gorm.DB {
				return tx.Session(&gorm.Session{DryRun: dry}).
					Table("representation_reference_option2").
					Where("resource_id = ?", resourceID).
					Where("common_representation_id IS NOT NULL").
					Update("common_representation_id", newCommonID)
			},
		)
		if err != nil {
			return timings, err
		}
	}

	if shouldInsertReporter {
		//fmt.Println("✅ Updating reporter rep")
		timings, err, _ = conditionalInsert(
			tx, timings, "update_ref_reporter", explain,
			func(dry bool) *gorm.DB {
				return tx.Session(&gorm.Session{DryRun: dry}).
					Table("representation_reference_option2").
					Where("resource_id = ?", resourceID).
					Where("reporter_representation_id IS NOT NULL").
					Update("reporter_representation_id", newReporterID)
			},
		)
		if err != nil {
			return timings, err
		}
	}

	return timings, nil
}

func insertRepresentationReferencesOption2(
	tx *gorm.DB,
	refs []option2models.RepresentationReference,
	dryRun bool,
) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(&refs)
}

func insertCommonRepresentationOption2(tx *gorm.DB, rep *option2models.CommonRepresentation, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(rep)
}

func insertReporterRepresentationOption2(tx *gorm.DB, rep *option2models.ReporterRepresentation, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(rep)
}

func insertResourceOption2(tx *gorm.DB, resource option2models.Resource, dryRun bool) *gorm.DB {
	return tx.Session(&gorm.Session{DryRun: dryRun}).Create(&resource)
}

func buildSelectRefsQueryOption2(
	tx *gorm.DB,
	rec benchmark.InputRecord,
	dryRun bool,
) (*gorm.DB, []option2models.JoinedRepresentation) {

	var results []option2models.JoinedRepresentation

	query := tx.Session(&gorm.Session{DryRun: dryRun}).
		Table("representation_reference_option2 AS ref").
		Joins(`
		LEFT JOIN reporter_representation_option2 AS rr 
		ON rr.id = ref.reporter_representation_id 
		AND rr.local_resource_id = ? 
		AND rr.reporter_instance_id = ? 
		AND rr.reporter_type = ? 
		AND rr.resource_type = ?
	`, rec.LocalResourceID, rec.ReporterInstanceID, rec.ReporterType, rec.ResourceType).
		Joins(`
		LEFT JOIN common_representation_option2 AS cr 
		ON cr.id = ref.common_representation_id
	`).
		Select(`
		ref.resource_id AS resource_id,
		rr.id AS reporter_id,
		rr.reporter_type AS reporter_reporter_type,
		rr.version AS reporter_version,
		rr.generation AS reporter_generation,
		rr.common_version AS reporter_common_version,
		rr.tombstone AS reporter_tombstone,
		cr.version AS common_version,
		cr.reporter_type AS common_reporter_type
	`)

	if !dryRun {
		//fmt.Println("⏳ Executing SELECT scan...")
		if err := query.Scan(&results).Error; err != nil {
			fmt.Printf("❌ Scan failed: %v\n", err)
		}
	}

	return query, results
}

func prepareJSON(input json.RawMessage, defaultVal map[string]string) datatypes.JSON {
	if len(input) == 0 {
		b, _ := json.Marshal(defaultVal)
		return b
	}
	return datatypes.JSON(input)
}

func conditionalInsert(
	tx *gorm.DB,
	timings []benchmark.StepTiming,
	label string,
	explain bool,
	execFunc func(dry bool) *gorm.DB,
) ([]benchmark.StepTiming, error, interface{}) {
	if explain {
		newTimings := benchmark.DryRunAndRecordExplainPlan(tx, timings, func() *gorm.DB {
			return execFunc(true)
		}, label)
		return newTimings, nil, nil // ✅ three values
	}

	return benchmark.ActualRunAndRecordExecutionTiming(tx, timings,
		func() (*gorm.DB, interface{}) {
			return execFunc(false), nil
		}, label)
}
