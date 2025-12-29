package parser

import (
	"os"
	"path/filepath"
	"testing"
)

func TestParseGTID(t *testing.T) {
	tests := []struct {
		name    string
		gtid    string
		wantErr bool
	}{
		{
			name:    "valid GTID",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
			wantErr: false,
		},
		{
			name:    "valid GTID with spaces",
			gtid:    "  3E11FA47-71CA-11E1-9E33-C80AA9429562:23  ",
			wantErr: false,
		},
		{
			name:    "empty GTID",
			gtid:    "",
			wantErr: true,
		},
		{
			name:    "invalid format - no colon",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562",
			wantErr: true,
		},
		{
			name:    "invalid format - bad UUID",
			gtid:    "invalid-uuid:23",
			wantErr: true,
		},
		{
			name:    "invalid format - bad transaction ID",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562:abc",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseGTID(tt.gtid)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseGTID() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestValidateGTIDFormat(t *testing.T) {
	tests := []struct {
		name    string
		gtid    string
		wantErr bool
	}{
		{
			name:    "valid format",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562:23",
			wantErr: false,
		},
		{
			name:    "empty string",
			gtid:    "",
			wantErr: true,
		},
		{
			name:    "no colon",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562",
			wantErr: true,
		},
		{
			name:    "short UUID",
			gtid:    "short:23",
			wantErr: true,
		},
		{
			name:    "multiple colons",
			gtid:    "3E11FA47-71CA-11E1-9E33-C80AA9429562:23:45",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateGTIDFormat(tt.gtid)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateGTIDFormat() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestParseGTIDFile(t *testing.T) {
	// Create temp directory for test files
	tmpDir := t.TempDir()

	tests := []struct {
		name     string
		content  string
		wantLen  int
		wantErr  bool
	}{
		{
			name: "valid file with multiple GTIDs",
			content: `3E11FA47-71CA-11E1-9E33-C80AA9429562:23
3E11FA47-71CA-11E1-9E33-C80AA9429562:24
3E11FA47-71CA-11E1-9E33-C80AA9429562:25`,
			wantLen: 3,
			wantErr: false,
		},
		{
			name: "file with comments and empty lines",
			content: `# This is a comment
3E11FA47-71CA-11E1-9E33-C80AA9429562:23

# Another comment
3E11FA47-71CA-11E1-9E33-C80AA9429562:24`,
			wantLen: 2,
			wantErr: false,
		},
		{
			name: "file with invalid GTID",
			content: `3E11FA47-71CA-11E1-9E33-C80AA9429562:23
invalid-gtid
3E11FA47-71CA-11E1-9E33-C80AA9429562:25`,
			wantLen: 0,
			wantErr: true,
		},
		{
			name:    "empty file",
			content: "",
			wantLen: 0,
			wantErr: true,
		},
		{
			name: "file with only comments",
			content: `# Comment 1
# Comment 2`,
			wantLen: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test file
			testFile := filepath.Join(tmpDir, tt.name+".txt")
			err := os.WriteFile(testFile, []byte(tt.content), 0644)
			if err != nil {
				t.Fatalf("Failed to create test file: %v", err)
			}

			// Test ParseGTIDFile
			gtids, err := ParseGTIDFile(testFile)
			if (err != nil) != tt.wantErr {
				t.Errorf("ParseGTIDFile() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && len(gtids) != tt.wantLen {
				t.Errorf("ParseGTIDFile() got %d GTIDs, want %d", len(gtids), tt.wantLen)
			}
		})
	}

	// Test non-existent file
	t.Run("non-existent file", func(t *testing.T) {
		_, err := ParseGTIDFile("/non/existent/file.txt")
		if err == nil {
			t.Error("ParseGTIDFile() expected error for non-existent file")
		}
	})
}
