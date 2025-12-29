package exporter

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/quyetmv/mysql-gtid-position/models"
)

func createTestPositions() []*models.GTIDPosition {
	return []*models.GTIDPosition{
		{
			BinlogFile: "/var/lib/mysql/mysql-bin.000001",
			Position:   12345,
			GTID:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
			Timestamp:  1703750400,
			CreatedAt:  time.Now(),
		},
		{
			BinlogFile: "/var/lib/mysql/mysql-bin.000002",
			Position:   67890,
			GTID:       "3E11FA47-71CA-11E1-9E33-C80AA9429562:24",
			Timestamp:  1703750500,
			CreatedAt:  time.Now(),
		},
	}
}

func TestCSVExporter_Export(t *testing.T) {
	tmpDir := t.TempDir()
	positions := createTestPositions()

	tests := []struct {
		name          string
		positions     []*models.GTIDPosition
		includeHeader bool
		wantErr       bool
	}{
		{
			name:          "export with header",
			positions:     positions,
			includeHeader: true,
			wantErr:       false,
		},
		{
			name:          "export without header",
			positions:     positions,
			includeHeader: false,
			wantErr:       false,
		},
		{
			name:          "export empty positions",
			positions:     []*models.GTIDPosition{},
			includeHeader: true,
			wantErr:       false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputFile := filepath.Join(tmpDir, tt.name+".csv")
			exporter := NewCSVExporter()
			exporter.IncludeHeader = tt.includeHeader

			err := exporter.Export(tt.positions, outputFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("CSVExporter.Export() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Verify file exists
				if _, err := os.Stat(outputFile); os.IsNotExist(err) {
					t.Errorf("Output file not created: %s", outputFile)
					return
				}

				// Read and verify CSV content
				file, err := os.Open(outputFile)
				if err != nil {
					t.Fatalf("Failed to open output file: %v", err)
				}
				defer file.Close()

				reader := csv.NewReader(file)
				records, err := reader.ReadAll()
				if err != nil {
					t.Fatalf("Failed to read CSV: %v", err)
				}

				expectedRows := len(tt.positions)
				if tt.includeHeader {
					expectedRows++
				}

				if len(records) != expectedRows {
					t.Errorf("Expected %d rows, got %d", expectedRows, len(records))
				}

				// Verify header if included
				if tt.includeHeader && len(records) > 0 {
					header := records[0]
					expectedHeader := []string{"binlog_file", "position", "gtid", "timestamp", "timestamp_readable"}
					for i, h := range header {
						if h != expectedHeader[i] {
							t.Errorf("Header[%d]: got %s, want %s", i, h, expectedHeader[i])
						}
					}
				}
			}
		})
	}
}

func TestCSVExporter_CustomDelimiter(t *testing.T) {
	tmpDir := t.TempDir()
	positions := createTestPositions()
	outputFile := filepath.Join(tmpDir, "custom_delimiter.csv")

	exporter := NewCSVExporter()
	exporter.Delimiter = ';'

	err := exporter.Export(positions, outputFile)
	if err != nil {
		t.Fatalf("CSVExporter.Export() error = %v", err)
	}

	// Read file and check delimiter
	content, err := os.ReadFile(outputFile)
	if err != nil {
		t.Fatalf("Failed to read file: %v", err)
	}

	if !strings.Contains(string(content), ";") {
		t.Error("Custom delimiter ';' not found in output")
	}
}

func TestJSONExporter_Export(t *testing.T) {
	tmpDir := t.TempDir()
	positions := createTestPositions()

	tests := []struct {
		name        string
		positions   []*models.GTIDPosition
		prettyPrint bool
		wantErr     bool
	}{
		{
			name:        "export pretty print",
			positions:   positions,
			prettyPrint: true,
			wantErr:     false,
		},
		{
			name:        "export compact",
			positions:   positions,
			prettyPrint: false,
			wantErr:     false,
		},
		{
			name:        "export empty positions",
			positions:   []*models.GTIDPosition{},
			prettyPrint: true,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			outputFile := filepath.Join(tmpDir, tt.name+".json")
			exporter := NewJSONExporter(tt.prettyPrint)

			err := exporter.Export(tt.positions, outputFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("JSONExporter.Export() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Verify file exists
				if _, err := os.Stat(outputFile); os.IsNotExist(err) {
					t.Errorf("Output file not created: %s", outputFile)
					return
				}

				// Read and verify JSON content
				content, err := os.ReadFile(outputFile)
				if err != nil {
					t.Fatalf("Failed to read file: %v", err)
				}

				var result map[string]interface{}
				if err := json.Unmarshal(content, &result); err != nil {
					t.Fatalf("Failed to parse JSON: %v", err)
				}

				// Verify structure
				if _, ok := result["total"]; !ok {
					t.Error("JSON missing 'total' field")
				}
				if _, ok := result["positions"]; !ok {
					t.Error("JSON missing 'positions' field")
				}
			}
		})
	}
}

func TestConsoleExporter_Export(t *testing.T) {
	positions := createTestPositions()
	exporter := NewConsoleExporter()

	// Test with positions
	err := exporter.Export(positions, "")
	if err != nil {
		t.Errorf("ConsoleExporter.Export() error = %v", err)
	}

	// Test with empty positions
	err = exporter.Export([]*models.GTIDPosition{}, "")
	if err != nil {
		t.Errorf("ConsoleExporter.Export() with empty positions error = %v", err)
	}
}

func TestConsoleExporter_ExportSingle(t *testing.T) {
	positions := createTestPositions()
	exporter := NewConsoleExporter()

	// Test with position
	err := exporter.ExportSingle(positions[0])
	if err != nil {
		t.Errorf("ConsoleExporter.ExportSingle() error = %v", err)
	}

	// Test with nil
	err = exporter.ExportSingle(nil)
	if err != nil {
		t.Errorf("ConsoleExporter.ExportSingle() with nil error = %v", err)
	}
}
