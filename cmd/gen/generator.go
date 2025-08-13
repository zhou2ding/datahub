package main

import "time"

// ColumnInfo holds data needed for template generation for a single column
type ColumnInfo struct {
	GoFieldName  string
	DBColumnName string
	DBDataType   string
}

// TemplateData holds all data needed for generating a single table's schema file
type TemplateData struct {
	Timestamp   time.Time
	PackageName string // Package name for the generated file
	DbName      string
	TableName   string
	StructName  string
	VarName     string
	Columns     []ColumnInfo
}
