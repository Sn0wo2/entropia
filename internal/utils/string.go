package utils

import (
	"fmt"
	"strings"
)

type Text struct{}

func (Text) String(value any) string {
	switch typed := value.(type) {
	case string:
		return strings.TrimSpace(typed)
	case fmt.Stringer:
		return typed.String()
	default:
		return ""
	}
}
