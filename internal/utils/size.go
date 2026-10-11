package utils

import (
	"fmt"
	"math"
	"strings"

	"github.com/dustin/go-humanize"
)

type Size struct{}

func (Size) Parse(value, unit string) (int64, error) {
	value = strings.TrimSpace(value)
	if value == "" || len(value) > 128 {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	if last := value[len(value)-1]; last >= '0' && last <= '9' || last == '.' {
		value += unit
	}
	bytes, err := humanize.ParseBytes(value)
	if err != nil || bytes > math.MaxInt64 {
		return 0, fmt.Errorf("invalid byte size %q", value)
	}
	return int64(bytes), nil
}
