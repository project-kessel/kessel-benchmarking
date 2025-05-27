package models

type ReporterRepresentation struct {
	BaseRepresentation

	ReporterVersion    string `gorm:"column:reporter_version"`
	ReporterInstanceID string `gorm:"size:256;column:reporter_instance_id"`
	APIHref            string `gorm:"size:256;column:api_href"`
	ConsoleHref        string `gorm:"size:256;column:console_href"`
	CommonVersion      int    `gorm:"column:common_version"`
	Tombstone          bool   `gorm:"column:tombstone"`
	Generation         int    `gorm:"column:generation"`
}

func (ReporterRepresentation) TableName() string {
	return "reporter_representation_option1"
}
