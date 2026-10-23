package utils

import (
	"fmt"
	"strings"
)

func String(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}
