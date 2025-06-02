package go_benchmark_tests

/*func benchmarkDenormalizedReferences2RepTablesBatched(b *testing.B) {

	db := config.ConnectDB()
	_ = db.AutoMigrate(
		&models.Resource{},
		&models.CommonRepresentation{},
		&models.ReporterRepresentation{},
		&models.RepresentationReference{},
	)

	wd, _ := os.Getwd()
	fmt.Println("Current working directory:", wd)

	records, err := benchmark.LoadInputRecords("input_files/input_1000_records.jsonl")
	if err != nil {
		b.Fatalf("failed to load input records: %v", err)
	}

	const batchSize = 10
	totalBatches := len(records) / batchSize
	if len(records)%batchSize != 0 {
		totalBatches++
	}

	for batchNum := 0; batchNum < totalBatches; batchNum++ {
		start := batchNum * batchSize
		end := start + batchSize
		if end > len(records) {
			end = len(records)
		}
		batch := records[start:end]

		b.Run(fmt.Sprintf("batch-%d", batchNum), func(b *testing.B) {
			for b.Loop() {
				for _, rec := range batch {
					err := db.Transaction(func(tx *gorm.DB) error {
						return benchmark.ProcessRecordOption1(tx, rec) // your extracted logic
					}, &sql.TxOptions{Isolation: sql.LevelSerializable})
					if err != nil {
						b.Errorf("transaction failed: %v", err)
					}
				}
			}
		})
	}
}*/

// add indexes, unique
