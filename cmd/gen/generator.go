package main

import (
	"bytes"
	"context"
	v1 "datahub/api/datalayer/v1"
	"fmt"
	"go/format"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc/credentials/insecure"

	"google.golang.org/grpc"
)

type ColumnInfo struct {
	GoFieldName  string
	DBColumnName string
	DBDataType   string
}

type TemplateData struct {
	Timestamp   time.Time
	PackageName string
	DbName      string
	TableName   string
	StructName  string
	VarName     string
	Columns     []ColumnInfo
}

type Generator struct {
	client  v1.MetadataClient
	outDir  string
	dbName  string
	pkgName string
}

func NewGenerator(addr, dbName, outDir string) (*Generator, error) {
	// 1. Setup gRPC Connection
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to gRPC server %s: %w", addr, err)
	}
	// Note: Consider closing the connection gracefully, maybe in main.go

	client := v1.NewMetadataClient(conn)

	// 2. Determine Package Name (use output directory base name)
	pkgName := filepath.Base(outDir)
	// Basic validation for package name
	if pkgName == "." || pkgName == "/" || strings.ContainsAny(pkgName, `/\:.`) {
		log.Printf("Warning: Output directory base '%s' is not a valid Go package name, using 'schema' instead.", pkgName)
		pkgName = "schema"
	}

	// 3. Ensure output directory exists
	if err = os.MkdirAll(outDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory %s: %w", outDir, err)
	}

	return &Generator{
		client:  client,
		outDir:  outDir,
		dbName:  dbName,
		pkgName: pkgName,
	}, nil
}

func (g *Generator) GenerateTables(ctx context.Context, tables []string) error {
	targetTables := tables
	var err error

	// If no specific tables provided, list all tables
	if len(targetTables) == 0 {
		log.Printf("No specific tables provided, listing tables for database '%s'...", g.dbName)
		listReq := &v1.ListTablesRequest{DbName: g.dbName}
		listResp, err := g.client.ListTables(ctx, listReq)
		if err != nil {
			return fmt.Errorf("failed to list tables for database %s: %w", g.dbName, err)
		}
		targetTables = listResp.GetTableNames()
		if len(targetTables) == 0 {
			log.Printf("No tables found in database '%s'.", g.dbName)
			return nil
		}
		log.Printf("Found tables: %v", targetTables)
	}

	// Generate code for each target table
	for _, tableName := range targetTables {
		log.Printf("Generating schema for table '%s.%s'...", g.dbName, tableName)
		err = g.generateSingleTable(ctx, tableName)
		if err != nil {
			log.Printf("Error generating schema for table %s: %v", tableName, err)
			// Decide whether to continue or stop on error
			// return err // Stop on first error
			continue // Log error and continue with next table
		}
	}

	log.Println("Code generation finished.")
	return nil
}

func (g *Generator) generateSingleTable(ctx context.Context, tableName string) error {
	// 1. Call DescribeTable RPC
	descReq := &v1.DescribeTableRequest{
		Table: &v1.TableSchema{ // Assuming you have TableSchema message
			DbName:    g.dbName,
			TableName: tableName,
		},
	}
	descResp, err := g.client.DescribeTable(ctx, descReq)
	if err != nil {
		return fmt.Errorf("DescribeTable RPC failed for %s: %w", tableName, err)
	}

	if descResp == nil || len(descResp.GetColumns()) == 0 {
		log.Printf("Warning: No columns found for table %s, skipping.", tableName)
		return nil
	}

	// 2. Prepare Template Data
	columns := make([]ColumnInfo, 0, len(descResp.GetColumns()))
	for _, col := range descResp.GetColumns() {
		columns = append(columns, ColumnInfo{
			GoFieldName:  simpleSnakeToCamel(col.GetName()),
			DBColumnName: col.GetName(),
			DBDataType:   col.GetDataType(),
		})
	}

	data := TemplateData{
		Timestamp:   time.Now(),
		PackageName: g.pkgName,
		DbName:      g.dbName,
		TableName:   tableName,
		StructName:  tableNameToStructName(tableName),
		VarName:     tableNameToVarName(tableName),
		Columns:     columns,
	}

	// 3. Execute Template
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, data); err != nil {
		return fmt.Errorf("failed to execute template for %s: %w", tableName, err)
	}

	// 4. Format Generated Code
	formattedSource, err := format.Source(buf.Bytes())
	if err != nil {
		log.Printf("Warning: Failed to format generated code for %s: %v. Writing unformatted code.", tableName, err)
		formattedSource = buf.Bytes() // Use unformatted code on error
	}

	// 5. Write to File
	outputFileName := fmt.Sprintf("%s_schema.go", strings.ToLower(tableName)) // e.g., users_schema.go
	outputFilePath := filepath.Join(g.outDir, outputFileName)

	err = os.WriteFile(outputFilePath, formattedSource, 0644)
	if err != nil {
		return fmt.Errorf("failed to write generated file %s: %w", outputFilePath, err)
	}

	log.Printf("Successfully generated %s", outputFilePath)
	return nil
}
