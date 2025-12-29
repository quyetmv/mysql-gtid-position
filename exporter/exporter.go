package exporter

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"os"

	"github.com/quyetmv/mysql-gtid-position/models"
)

// Exporter interface for different output formats
type Exporter interface {
	Export(positions []*models.GTIDPosition, output string) error
}

// CSVExporter exports results to CSV format
type CSVExporter struct {
	IncludeHeader bool
	Delimiter     rune
}

// NewCSVExporter creates a new CSV exporter
func NewCSVExporter() *CSVExporter {
	return &CSVExporter{
		IncludeHeader: true,
		Delimiter:     ',',
	}
}

// Export writes GTID positions to CSV file
func (e *CSVExporter) Export(positions []*models.GTIDPosition, output string) error {
	var file *os.File
	var err error

	if output == "" || output == "-" {
		file = os.Stdout
	} else {
		file, err = os.Create(output)
		if err != nil {
			return fmt.Errorf("failed to create CSV file: %w", err)
		}
		defer file.Close()
	}

	writer := csv.NewWriter(file)
	writer.Comma = e.Delimiter
	defer writer.Flush()

	// Write header
	if e.IncludeHeader {
		header := []string{"binlog_file", "position", "gtid", "timestamp", "timestamp_readable"}
		if err := writer.Write(header); err != nil {
			return fmt.Errorf("failed to write CSV header: %w", err)
		}
	}

	// Write data rows
	for _, pos := range positions {
		row := []string{
			pos.BinlogFile,
			fmt.Sprintf("%d", pos.Position),
			pos.GTID,
			fmt.Sprintf("%d", pos.Timestamp),
			pos.TimestampReadable(),
		}
		if err := writer.Write(row); err != nil {
			return fmt.Errorf("failed to write CSV row: %w", err)
		}
	}

	return nil
}

// JSONExporter exports results to JSON format
type JSONExporter struct {
	PrettyPrint bool
}

// NewJSONExporter creates a new JSON exporter
func NewJSONExporter(prettyPrint bool) *JSONExporter {
	return &JSONExporter{
		PrettyPrint: prettyPrint,
	}
}

// Export writes GTID positions to JSON file
func (e *JSONExporter) Export(positions []*models.GTIDPosition, output string) error {
	var file *os.File
	var err error

	if output == "" || output == "-" {
		file = os.Stdout
	} else {
		file, err = os.Create(output)
		if err != nil {
			return fmt.Errorf("failed to create JSON file: %w", err)
		}
		defer file.Close()
	}

	encoder := json.NewEncoder(file)
	if e.PrettyPrint {
		encoder.SetIndent("", "  ")
	}

	// Wrap in result object
	result := map[string]interface{}{
		"total":     len(positions),
		"positions": positions,
	}

	if err := encoder.Encode(result); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}
