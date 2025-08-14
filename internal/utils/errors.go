package utils

import "strings"

// IsUniqueViolation checks if an error is a database unique constraint violation
func IsUniqueViolation(err error) bool {
	return err != nil && (strings.Contains(err.Error(), "duplicate key") ||
		strings.Contains(err.Error(), "unique constraint") ||
		strings.Contains(err.Error(), "unique"))
}
