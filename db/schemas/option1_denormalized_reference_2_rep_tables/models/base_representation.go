package models

import (
	"gorm.io/datatypes"
)

type BaseRepresentation struct {
	Data datatypes.JSON `gorm:"type:jsonb;column:data"`
}
