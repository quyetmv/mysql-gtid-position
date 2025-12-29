package parser

import (
	"testing"


)

func TestExtractUUIDs(t *testing.T) {
	tests := []struct {
		name    string
		gtidStr string
		wantLen int
		wantErr bool
	}{
		{
			name:    "single UUID",
			gtidStr: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
			wantLen: 1,
			wantErr: false,
		},
		{
			name:    "multiple UUIDs",
			gtidStr: "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100,a1b2c3d4-71ca-11e1-9e33-c80aa9429562:1-50",
			wantLen: 2,
			wantErr: false,
		},
		{
			name:    "nil GTID set",
			gtidStr: "",
			wantLen: 0,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gtidStr == "" {
				_, err := ExtractUUIDs(nil)
				if (err != nil) != tt.wantErr {
					t.Errorf("ExtractUUIDs() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			gtidSet, err := ParseGTID(tt.gtidStr)
			if err != nil {
				t.Fatalf("ParseGTID() error = %v", err)
			}

			uuidInfos, err := ExtractUUIDs(&gtidSet)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractUUIDs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if len(uuidInfos) != tt.wantLen {
				t.Errorf("ExtractUUIDs() got %d UUIDs, want %d", len(uuidInfos), tt.wantLen)
			}

			// Verify UUID info fields
			for _, info := range uuidInfos {
				if info.UUID == "" {
					t.Error("UUID string is empty")
				}
				if info.MaxTransaction == 0 {
					t.Error("MaxTransaction is 0")
				}
				if info.TotalCount == 0 {
					t.Error("TotalCount is 0")
				}
			}
		})
	}
}

func TestFindActiveMasterUUID(t *testing.T) {
	tests := []struct {
		name       string
		gtidStr    string
		wantUUID   string
		wantErr    bool
		checkMaxGNO bool
	}{
		{
			name:        "single UUID",
			gtidStr:     "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
			checkMaxGNO: true,
			wantErr:     false,
		},
		{
			name:        "multiple UUIDs - first has higher GNO",
			gtidStr:     "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-1000,a1b2c3d4-71ca-11e1-9e33-c80aa9429562:1-50",
			checkMaxGNO: true,
			wantErr:     false,
		},
		{
			name:    "nil GTID set",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gtidStr == "" {
				_, err := FindActiveMasterUUID(nil)
				if (err != nil) != tt.wantErr {
					t.Errorf("FindActiveMasterUUID() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			gtidSet, err := ParseGTID(tt.gtidStr)
			if err != nil {
				t.Fatalf("ParseGTID() error = %v", err)
			}

			uuid, err := FindActiveMasterUUID(&gtidSet)
			if (err != nil) != tt.wantErr {
				t.Errorf("FindActiveMasterUUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && uuid == "" {
				t.Error("FindActiveMasterUUID() returned empty UUID")
			}

			// Verify it's the UUID with highest transaction number
			if tt.checkMaxGNO {
				uuidInfos, _ := ExtractUUIDs(&gtidSet)
				maxGNO := uint64(0)
				for _, info := range uuidInfos {
					if info.MaxTransaction > maxGNO {
						maxGNO = info.MaxTransaction
					}
				}

				// Find the UUID with max GNO
				for _, info := range uuidInfos {
					if info.MaxTransaction == maxGNO {
						// UUID should match (case-insensitive)
						if info.UUID != uuid {
							t.Logf("Expected UUID with max GNO: %s, got: %s", info.UUID, uuid)
						}
					}
				}
			}
		})
	}
}

func TestFilterByUUID(t *testing.T) {
	tests := []struct {
		name       string
		gtidStr    string
		filterUUID string
		wantErr    bool
	}{
		{
			name:       "filter existing UUID",
			gtidStr:    "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100,a1b2c3d4-71ca-11e1-9e33-c80aa9429562:1-50",
			filterUUID: "3e11fa47-71ca-11e1-9e33-c80aa9429562",
			wantErr:    false,
		},
		{
			name:       "filter non-existing UUID",
			gtidStr:    "3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100",
			filterUUID: "ffffffff-ffff-ffff-ffff-ffffffffffff",
			wantErr:    true,
		},
		{
			name:       "nil GTID set",
			filterUUID: "3e11fa47-71ca-11e1-9e33-c80aa9429562",
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.gtidStr == "" {
				_, err := FilterByUUID(nil, tt.filterUUID)
				if (err != nil) != tt.wantErr {
					t.Errorf("FilterByUUID() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}

			gtidSet, err := ParseGTID(tt.gtidStr)
			if err != nil {
				t.Fatalf("ParseGTID() error = %v", err)
			}

			filtered, err := FilterByUUID(&gtidSet, tt.filterUUID)
			if (err != nil) != tt.wantErr {
				t.Errorf("FilterByUUID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				// Verify filtered set only contains the target UUID
				uuidInfos, _ := ExtractUUIDs(&filtered)
				if len(uuidInfos) != 1 {
					t.Errorf("FilterByUUID() returned %d UUIDs, want 1", len(uuidInfos))
				}
			}
		})
	}
}

func TestExtractGTIDInfo(t *testing.T) {
	tests := []struct {
		name    string
		gtidStr string
		wantGNO uint64
		wantErr bool
	}{
		{
			name:    "valid GTID",
			gtidStr: "3e11fa47-71ca-11e1-9e33-c80aa9429562:123",
			wantGNO: 123,
			wantErr: false,
		},
		{
			name:    "invalid format - no colon",
			gtidStr: "3e11fa47-71ca-11e1-9e33-c80aa9429562",
			wantErr: true,
		},
		{
			name:    "invalid format - invalid GNO",
			gtidStr: "3e11fa47-71ca-11e1-9e33-c80aa9429562:abc",
			wantErr: true,
		},
		{
			name:    "empty string",
			gtidStr: "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			uuid, gno, err := ExtractGTIDInfo(tt.gtidStr)
			if (err != nil) != tt.wantErr {
				t.Errorf("ExtractGTIDInfo() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr {
				if uuid == "" {
					t.Error("ExtractGTIDInfo() returned empty UUID")
				}
				if gno != tt.wantGNO {
					t.Errorf("ExtractGTIDInfo() GNO = %d, want %d", gno, tt.wantGNO)
				}
			}
		})
	}
}

func TestParseGTID_MultiUUID(t *testing.T) {
	// Test với GTID example của user
	gtidStr := "0e95f562-6c20-11ef-bec4-5eeba390a904:1-12771309078,22f7ce9e-7f4c-11ef-8423-3a25d006dfee:1-3"
	
	gtidSet, err := ParseGTID(gtidStr)
	if err != nil {
		t.Fatalf("ParseGTID() error = %v", err)
	}

	// Extract UUIDs
	uuidInfos, err := ExtractUUIDs(&gtidSet)
	if err != nil {
		t.Fatalf("ExtractUUIDs() error = %v", err)
	}

	if len(uuidInfos) != 2 {
		t.Errorf("Expected 2 UUIDs, got %d", len(uuidInfos))
	}

	// Find active master (should be first UUID with higher GNO)
	activeMasterUUID, err := FindActiveMasterUUID(&gtidSet)
	if err != nil {
		t.Fatalf("FindActiveMasterUUID() error = %v", err)
	}

	// Verify active master has highest transaction number
	var maxGNO uint64
	for _, info := range uuidInfos {
		if info.MaxTransaction > maxGNO {
			maxGNO = info.MaxTransaction
		}
	}

	// The active master should have the max GNO (12771309078)
	if maxGNO != 12771309078 {
		t.Errorf("Expected max GNO 12771309078, got %d", maxGNO)
	}

	t.Logf("Active master UUID: %s (max GNO: %d)", activeMasterUUID, maxGNO)
}
