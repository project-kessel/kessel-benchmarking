package benchmark_input

import (
	"fmt"
	"math/rand"
	"time"
)

func Categorize(idNum uint64) string {
	switch {
	case idNum < 30:
		return "cat1"
	case idNum < 60:
		return "cat2"
	case idNum < 70:
		return "cat3"
	case idNum < 80:
		return "cat4"
	case idNum < 90:
		return "cat5"
	default:
		return "cat6"
	}
}

// GenerateZipfIDsWithModuloCategory generates Zipf IDs and maps them to categories after applying modulo
func GenerateZipfIDsWithModuloCategory(n int, zipfMax uint64, modBase uint64, s, v float64) map[string][]string {
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	zipf := rand.NewZipf(r, s, v, zipfMax)

	categorized := map[string][]string{
		"cat1": {}, "cat2": {}, "cat3": {},
		"cat4": {}, "cat5": {}, "cat6": {},
	}

	for i := 0; i < n; i++ {
		raw := zipf.Uint64()   // e.g., 0–999
		modID := raw % modBase // force to 0–99
		id := fmt.Sprintf("id-%d", modID)
		cat := Categorize(modID)
		fmt.Println(id)
		categorized[cat] = append(categorized[cat], id)
	}

	return categorized
}
