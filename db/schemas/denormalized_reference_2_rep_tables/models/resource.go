package models

import "github.com/google/uuid"

type Resource struct {
	ID   uuid.UUID `gorm:"type:uuid;primaryKey"`
	Type string    `gorm:"size:128"`
}

func (Resource) TableName() string {
	return "resources_option1"
}
