package models

type CommonRepresentation struct {
	BaseRepresentation
}

func (CommonRepresentation) TableName() string {
	return "common_representation_option1"
}
