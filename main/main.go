// cmd/zipfgen/main.go
package main

import (
	"fmt"
	"github.com/yourusername/go-db-bench/benchmark/benchmark_input"
)

func main() {

	totalSamples := 1000
	zipfMax := uint64(1000) // Zipf can generate values 0–999
	modBase := uint64(100)  // Reduce to 0–99 for category mapping
	alpha := 1.3
	xm := 1.0
	categories := benchmark_input.GenerateZipfIDsWithModuloCategory(totalSamples, zipfMax, modBase, alpha, xm)

	// Print summary
	for cat, ids := range categories {
		fmt.Printf("%s: %d ids\n", cat, len(ids))
	}

}
