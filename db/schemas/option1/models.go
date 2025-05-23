package option1

import (
    "github.com/google/uuid"
    "gorm.io/datatypes"
)

type Resource struct {
    ID   uuid.UUID `gorm:"type:uuid;primaryKey"`
    Type string    `gorm:"size:128"`
}

type BaseRepresentation struct {
    LocalResourceID uuid.UUID      `gorm:"type:uuid"`
    ReporterType    string         `gorm:"size:128"`
    ResourceType    string         `gorm:"size:128"`
    Version         int
    Data            datatypes.JSON `gorm:"type:jsonb"`
}

type ReporterRepresentation struct {
    BaseRepresentation
    ReporterVersion    string `gorm:"column:reporter_version"`
    ReporterInstanceID string `gorm:"column:reporter_instance_id;size:256"`
    APIHref            string `gorm:"column:api_href;size:256"`
    ConsoleHref        string `gorm:"column:console_href;size:256"`
    CommonVersion      int
    Tombstone          bool
    Generation         int
}

type CommonRepresentation struct {
    BaseRepresentation
}

type RepresentationReference struct {
    ResourceID            uuid.UUID
    LocalResourceID       uuid.UUID
    ReporterType          string
    ReporterInstanceID    string
    RepresentationVersion int
    Generation            int
}
