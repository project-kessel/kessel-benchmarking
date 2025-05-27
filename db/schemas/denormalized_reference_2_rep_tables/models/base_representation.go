package models

import (
	"gorm.io/datatypes"
)

type BaseRepresentation struct {
	LocalResourceID string         `gorm:"column:local_resource_id"`
	ReporterType    string         `gorm:"size:128;column:reporter_type"`
	ResourceType    string         `gorm:"size:128;column:resource_type"`
	Version         int            `gorm:"column:version"`
	Data            datatypes.JSON `gorm:"type:jsonb;column:data"`
}
