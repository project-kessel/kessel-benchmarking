package models

type CommonRepresentation struct {
	BaseRepresentation
	ReportedBy string `gorm:"column:reported_by"`
}

func (CommonRepresentation) TableName() string {
	return "common_representation_option1"
}
