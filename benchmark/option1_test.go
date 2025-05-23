package benchmark

import (
    "testing"
    "github.com/yourusername/go-db-bench/config"
    "github.com/yourusername/go-db-bench/db/schemas/option1"
    "gorm.io/gorm"
    "github.com/google/uuid"
    "encoding/json"
)

var dbConn *gorm.DB

func init() {
    dbConn = config.ConnectDB()
    dbConn.AutoMigrate(&option1.Resource{}, &option1.ReporterRepresentation{}, &option1.CommonRepresentation{}, &option1.RepresentationReference{})
}

func BenchmarkWriteResourceOption1(b *testing.B) {
    for i := 0; i < b.N; i++ {
        res := option1.Resource{
            ID:   uuid.New(),
            Type: "host",
        }
        dbConn.Create(&res)
    }
}

func BenchmarkWriteReporterRepresentationOption1(b *testing.B) {
    for i := 0; i < b.N; i++ {
        data, _ := json.Marshal(map[string]string{"foo": "bar"})
        rep := option1.ReporterRepresentation{
            BaseRepresentation: option1.BaseRepresentation{
                LocalResourceID: uuid.New(),
                ReporterType:    "hbi",
                ResourceType:    "host",
                Version:         1,
                Data:            data,
            },
            ReporterVersion:    "123.2",
            ReporterInstanceID: "abc",
            APIHref:            "https://example.com",
            ConsoleHref:        "https://console.example.com",
            CommonVersion:      1,
            Tombstone:          false,
            Generation:         1,
        }
        dbConn.Create(&rep)
    }
}
