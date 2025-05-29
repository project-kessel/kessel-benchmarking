package models

import "github.com/google/uuid"

type RepresentationReference struct {
	ResourceID       uuid.UUID `gorm:"type:uuid;column:resource_id;index:unique"`
	RepresentationId uuid.UUID `gorm:"type:uuid;column:representation_id;index:unique"`
}

func (RepresentationReference) TableName() string {
	return "representation_references_option3"
}
