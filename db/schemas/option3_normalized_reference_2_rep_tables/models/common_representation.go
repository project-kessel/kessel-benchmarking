package models

import "github.com/google/uuid"

type CommonRepresentation struct {
	ID uuid.UUID `gorm:"type:uuid;primaryKey"`
	BaseRepresentation
	ReportedBy string `gorm:"column:reported_by"`
}

func (CommonRepresentation) TableName() string {
	return "common_representation_option3"
}
