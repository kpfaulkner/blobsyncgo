package azureutils

import (
	"github.com/edsrzf/mmap-go"
	"math"
)

// populate buffer...   make sure we do NOT go over maxOffset (inclusive)
// way too much casting crap here.
func PopulateBuffer(mm *mmap.MMap, offset int64, length int64, maxOffset int64) ([]byte,error) {
	endOffset := int64(math.Min(float64(offset + length ), float64(maxOffset+1)))
	buffer := (*mm)[offset:endOffset]
	return buffer, nil
}
