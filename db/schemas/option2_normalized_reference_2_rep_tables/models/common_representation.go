package models

import "github.com/google/uuid"

type CommonRepresentation struct {
	BaseRepresentation
	ID              uuid.UUID `gorm:"type:uuid;primaryKey"`
	LocalResourceID string    `gorm:"column:local_resource_id;index:unique_common_rep_idx,unique"`
	Version         int       `gorm:"column:version;index:unique_common_rep_idx,unique"`
	ReportedBy      string    `gorm:"column:reported_by"`
	ReporterType    string    `gorm:"size:128;column:reporter_type"`
	ResourceType    string    `gorm:"size:128;column:resource_type"`
}

func (CommonRepresentation) TableName() string {
	return "common_representation_option2"
}
