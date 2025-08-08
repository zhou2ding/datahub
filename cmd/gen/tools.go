package gen

import (
	"strings"
	"unicode"
)

func simpleSnakeToCamel(s string) string {
	if s == "" {
		return ""
	}
	if strings.ToLower(s) == "id" {
		return "ID"
	}

	var result strings.Builder
	capitalizeNext := true

	for _, r := range s {
		if r == '_' {
			capitalizeNext = true
		} else if capitalizeNext {
			result.WriteRune(unicode.ToUpper(r))
			capitalizeNext = false
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func tableNameToStructName(tableName string) string {
	return simpleSnakeToCamel(tableName) + "Table"
}

func tableNameToVarName(tableName string) string {
	return simpleSnakeToCamel(tableName)
}
