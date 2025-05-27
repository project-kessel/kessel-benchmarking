package models

import "github.com/google/uuid"

type RepresentationReference struct {
	ResourceID            uuid.UUID `gorm:"type:uuid;column:resource_id"`
	LocalResourceID       string    `gorm:"column:local_resource_id"`
	ReporterType          string    `gorm:"column:reporter_type"`
	ResourceType          string    `gorm:"column:resource_type"`
	ReporterInstanceID    string    `gorm:"column:reporter_instance_id"`
	RepresentationVersion int       `gorm:"column:representation_version"`
	Generation            int       `gorm:"column:generation"`
	Tombstone             bool      `gorm:"column:tombstone"`
}

func (RepresentationReference) TableName() string {
	return "representation_references_option1"
}
