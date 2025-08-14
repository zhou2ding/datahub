package main

import (
	v1 "datahub/api/datalayer/v1"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
