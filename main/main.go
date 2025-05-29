// cmd/zipfgen/main.go
package main

import (
	"fmt"
	"github.com/yourusername/go-db-bench/benchmark/input_files"
)

func main() {

	totalSamples := 10000
	zipfMax := uint64(100) // Zipf can generate values 0–99
	modBase := uint64(100) // Reduce to 0–99 for category mapping
	alpha := 1.3
	xm := 1.0
	outputPath := "/Users/snehagunta/git/kessel/kessel-benchmarking/benchmark/input_files/input_10000_records.jsonl"
	categories := input_files.GenerateZipfIDsWithModuloCategory(totalSamples, zipfMax, modBase, alpha, xm, outputPath)

	// Print summary
	for cat, ids := range categories {
		fmt.Printf("%s: %d ids\n", cat, len(ids))
	}

}
