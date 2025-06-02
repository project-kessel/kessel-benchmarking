package go_benchmark_tests

/*func BenchmarkDenormalizedReferences2RepTables(b *testing.B) {

	db := config.ConnectDB()
	if err := config.ResetDatabase(db); err != nil {
		b.Fatalf("failed to reset DB: %v", err)
	}

	wd, _ := os.Getwd()
	fmt.Println("Current working directory:", wd)

	records, err := benchmark.LoadInputRecords("input_files/input_1000_records.jsonl")
	if err != nil {
		b.Fatalf("failed to load input records: %v", err)
	}

	for b.Loop() {
		for _, rec := range records {
			err := db.Transaction(func(tx *gorm.DB) error {
				return .ProcessRecordOption1(tx, rec)
			}, &sql.TxOptions{Isolation: sql.LevelSerializable})
			if err != nil {
				b.Errorf("transaction failed: %v", err)
			}
		}
	}
}*/

// add indexes, unique
