package models

import "github.com/google/uuid"

type JoinedRepresentation struct {
	ResourceID uuid.UUID              `gorm:"column:resource_id"`
	Reporter   ReporterRepresentation `gorm:"embedded;embeddedPrefix:reporter_"`
	Common     CommonRepresentation   `gorm:"embedded;embeddedPrefix:common_"`
}
