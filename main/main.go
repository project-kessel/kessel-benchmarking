// cmd/zipfgen/main.go
package main

import (
	"fmt"
	"github.com/yourusername/go-db-bench/benchmark/benchmark_input"
)

func main() {

	totalSamples := 10
	zipfMax := uint64(10) // Zipf can generate values 0–99
	modBase := uint64(10) // Reduce to 0–99 for category mapping
	alpha := 1.3
	xm := 1.0
	outputPath := "/Users/snehagunta/git/kessel/kessel-benchmarking/benchmark/benchmark_input/input_10_records.jsonl"
	categories := benchmark_input.GenerateZipfIDsWithModuloCategory(totalSamples, zipfMax, modBase, alpha, xm, outputPath)

	// Print summary
	for cat, ids := range categories {
		fmt.Printf("%s: %d ids\n", cat, len(ids))
	}

}
