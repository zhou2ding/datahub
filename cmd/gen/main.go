package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"
)

func main() {
	grpcAddr := flag.String("addr", "localhost:10115", "gRPC server address")
	dbName := flag.String("db", "", "Database name (required)")
	outDir := flag.String("out", "./internal/entity", "Output directory for generated code")
	tablesStr := flag.String("tables", "", "Comma-separated list of specific tables to generate (optional, default: all)")

	flag.Parse()

	if *dbName == "" {
		log.Printf("Error: -db flag (Database name) is required.")
		flag.Usage()
		os.Exit(1)
	}
	if *grpcAddr == "" {
		log.Printf("Error: -addr flag (gRPC server address) is required.")
		flag.Usage()
		os.Exit(1)
	}
	if *outDir == "" {
		log.Printf("Error: -out flag (Output directory) is required.")
		flag.Usage()
		os.Exit(1)
	}

	err := os.MkdirAll(*outDir, 0755)
	if err != nil {
		log.Fatal(err)
	}

	var specificTables []string
	if *tablesStr != "" {
		specificTables = strings.Split(*tablesStr, ",")
		for i := range specificTables {
			specificTables[i] = strings.TrimSpace(specificTables[i])
		}
		log.Printf("Target specific tables: %v", specificTables)
	}

	generator, err := NewGenerator(*grpcAddr, *dbName, *outDir)
	if err != nil {
		log.Fatalf("Error creating generator: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	err = generator.GenerateTables(ctx, specificTables)
	if err != nil {
		os.Exit(1)
	}

	log.Println("Code generation completed successfully.")
}
