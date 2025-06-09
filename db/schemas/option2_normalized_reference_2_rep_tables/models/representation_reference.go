package models

import "github.com/google/uuid"

type RepresentationReference struct {
	ResourceID               uuid.UUID  `gorm:"type:uuid;not null"`
	ReporterRepresentationID *uuid.UUID `gorm:"type:uuid"`
	CommonRepresentationID   *uuid.UUID `gorm:"type:uuid"`
}

func (RepresentationReference) TableName() string {
	return "representation_reference_option2"
}
