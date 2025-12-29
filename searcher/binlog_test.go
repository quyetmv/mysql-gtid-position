package searcher

import (

	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/quyetmv/mysql-gtid-position/models"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

func TestGetBinlogFiles(t *testing.T) {
	// Create temp directory with test files
	tmpDir := t.TempDir()

	// Create test binlog files
	testFiles := []string{
		"mysql-bin.000001",
		"mysql-bin.000002",
		"mysql-bin.000003",
		"mysql-bin.index", // Should be filtered out
		"other-file.txt",  // Should not match pattern
	}

	for _, f := range testFiles {
		path := filepath.Join(tmpDir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	cfg := &models.Config{
		BinlogDir:   tmpDir,
		FilePattern: "mysql-bin.*",
	}
	searcher := NewSearcher(cfg)

	tests := []struct {
		name    string
		pattern string
		wantLen int
		wantErr bool
	}{
		{
			name:    "match binlog files",
			pattern: "mysql-bin.*",
			wantLen: 3, // Should exclude .index file
			wantErr: false,
		},
		{
			name:    "match all files",
			pattern: "*",
			wantLen: 4, // All files except .index
			wantErr: false,
		},
		{
			name:    "no match",
			pattern: "nonexistent.*",
			wantLen: 0,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files, err := searcher.GetBinlogFiles(tmpDir, tt.pattern)
			if (err != nil) != tt.wantErr {
				t.Errorf("GetBinlogFiles() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Count files excluding .index
			count := 0
			for _, f := range files {
				if filepath.Ext(f) != ".index" {
					count++
				}
			}

			if count < tt.wantLen {
				t.Errorf("GetBinlogFiles() got %d files, want at least %d", count, tt.wantLen)
			}
		})
	}
}

func TestNewSearcher(t *testing.T) {
	cfg := &models.Config{
		BinlogDir:   "/test/dir",
		TargetGTID:  "test-gtid",
		FilePattern: "mysql-bin.*",
		Parallel:    4,
		Verbose:     true,
	}

	searcher := NewSearcher(cfg)

	if searcher == nil {
		t.Fatal("NewSearcher() returned nil")
	}

	if searcher.config != cfg {
		t.Error("NewSearcher() config not set correctly")
	}

	if searcher.verbose != cfg.Verbose {
		t.Error("NewSearcher() verbose flag not set correctly")
	}
}

func TestGetBinlogFiles_Sorting(t *testing.T) {
	tmpDir := t.TempDir()

	// Create files in non-sorted order
	testFiles := []string{
		"mysql-bin.000003",
		"mysql-bin.000001",
		"mysql-bin.000002",
	}

	for _, f := range testFiles {
		path := filepath.Join(tmpDir, f)
		if err := os.WriteFile(path, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file: %v", err)
		}
	}

	cfg := &models.Config{
		BinlogDir: tmpDir,
	}
	searcher := NewSearcher(cfg)

	files, err := searcher.GetBinlogFiles(tmpDir, "mysql-bin.*")
	if err != nil {
		t.Fatalf("GetBinlogFiles() error = %v", err)
	}

	if len(files) != 3 {
		t.Fatalf("Expected 3 files, got %d", len(files))
	}

	// Check if sorted
	expected := []string{
		filepath.Join(tmpDir, "mysql-bin.000001"),
		filepath.Join(tmpDir, "mysql-bin.000002"),
		filepath.Join(tmpDir, "mysql-bin.000003"),
	}

	for i, f := range files {
		if f != expected[i] {
			t.Errorf("File at index %d: got %s, want %s", i, f, expected[i])
		}
	}
}

// MockBinlogParser for testing
type MockBinlogParser struct {
	events []interface{} // Can be specific events or errors
	forcedError error
}

func (m *MockBinlogParser) ParseFile(name string, offset int64, execution replication.OnEventFunc) error {
	if m.forcedError != nil {
		return m.forcedError
	}

	for _, evt := range m.events {
		if err, ok := evt.(error); ok {
			return err
		}

		if event, ok := evt.(*replication.BinlogEvent); ok {
			if err := execution(event); err != nil {
				return err
			}
		}
	}
	return nil
}

func createGTIDEvent(uuidStr string, gno int64) *replication.BinlogEvent {
	// Parse UUID
	// Format: 3E11FA47-71CA-11E1-9E33-C80AA9429562
	// We need to convert this to [16]byte for SID
	
	// Simplify for test: just create partial event that satisfies the code
	// The code expects:
	// e.Header.EventType == replication.GTID_EVENT
	// e.Event.(*replication.GTIDEvent)
	// gtidEvent.SID (16 bytes)
	// gtidEvent.GNO (int64)

	header := &replication.EventHeader{
		EventType: replication.GTID_EVENT,
		LogPos:    1000,
		EventSize: 100,
		Timestamp: uint32(time.Now().Unix()),
	}

	// Manual UUID parsing for test
	var sid [16]byte
	fmt.Sscanf(uuidStr, "%2x%2x%2x%2x-%2x%2x-%2x%2x-%2x%2x-%2x%2x%2x%2x%2x%2x",
		&sid[0], &sid[1], &sid[2], &sid[3],
		&sid[4], &sid[5],
		&sid[6], &sid[7],
		&sid[8], &sid[9],
		&sid[10], &sid[11], &sid[12], &sid[13], &sid[14], &sid[15])

	gtidEvent := &replication.GTIDEvent{
		SequenceNumber: 1,
		CommitFlag:     1,
	}
	gtidEvent.SID = sid[:]
	gtidEvent.GNO = gno

	return &replication.BinlogEvent{
		Header: header,
		Event:  gtidEvent,
	}
}

func TestSearchBinlogFile_Found(t *testing.T) {
	// Setup
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// Create GTID event
	gtidEvent := createGTIDEvent(targetUUID, 10)
	
	// Create XID event to mark transaction end
	xidEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000, // End position
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{
			XID: 123,
		},
	}

	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent,
			xidEvent, // Transaction end
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	// Test
	result, err := searcher.searchBinlogFile("dummy-file", &targetGTID)

	// Verify
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}
	if result.GTID != fmt.Sprintf("%s:10", targetUUID) {
		t.Errorf("Expected GTID %s:10, got %s", targetUUID, result.GTID)
	}
	// Position should be start of transaction (GTID event start)
	if result.Position != 900 {
		t.Errorf("Expected start position 900 (GTID start), got %d", result.Position)
	}
	// CommitPosition should be end of transaction (XID LogPos)
	if result.CommitPosition != 2000 {
		t.Errorf("Expected commit position 2000 (XID end), got %d", result.CommitPosition)
	}
}

func TestSearchBinlogFile_NotFound(t *testing.T) {
	// Setup
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:100-200", targetUUID) // Range 100-200
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	mockParser := &MockBinlogParser{
		events: []interface{}{
			createGTIDEvent(targetUUID, 10), // Outside range (10 < 100)
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	// Test
	result, err := searcher.searchBinlogFile("dummy-file", &targetGTID)

	// Verify
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("Expected nil result, got %v", result)
	}
}

func TestSearchBinlogFile_Error(t *testing.T) {
	// Setup
	targetGTID, _ := mysql.ParseMysqlGTIDSet("3e11fa47-71ca-11e1-9e33-c80aa9429562:1-100")
	expectedErr := fmt.Errorf("read error")

	mockParser := &MockBinlogParser{
		forcedError: expectedErr,
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	// Test
	_, err := searcher.searchBinlogFile("dummy-file", &targetGTID)

	// Verify
	if err == nil {
		t.Error("Expected error, got nil")
	} else if err != expectedErr {
		t.Errorf("Expected error %v, got %v", expectedErr, err)
	}
}

func TestSearchParallel(t *testing.T) {
	// Setup
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// File 1: Not found
	// File 2: Found
	// File 3: Not scanned (should be cancelled)

	xidEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 123},
	}

	mockParserFound := &MockBinlogParser{
		events: []interface{}{
			createGTIDEvent(targetUUID, 50),
			xidEvent,
		},
	}
	mockParserNotFound := &MockBinlogParser{
		events: []interface{}{
			createGTIDEvent(targetUUID, 200),
			xidEvent,
		},
	}

	searcher := &Searcher{
		config: &models.Config{
			Parallel: 2,
			Verbose:  true,
		},
		parserFactory: func() BinlogParser {
			return &MockBinlogParser{
				events: []interface{}{}, // Default empty
			}
		},
	}
	
	smartMockParser := &SmartMockParser{
		files: map[string]*MockBinlogParser{
			"file1": mockParserNotFound,
			"file2": mockParserFound,
			"file3": mockParserNotFound,
		},
	}
	
	searcher.parserFactory = func() BinlogParser {
		return smartMockParser
	}

	files := []string{"file1", "file2", "file3"}

	// Test
	result, err := searcher.SearchParallel(files, &targetGTID)

	// Verify
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}
	if result.BinlogFile != "file2" {
		t.Errorf("Expected result from file2, got %s", result.BinlogFile)
	}
}

// SmartMockParser dispatches to other mocks based on filename
type SmartMockParser struct {
	files map[string]*MockBinlogParser
}

func (m *SmartMockParser) ParseFile(name string, offset int64, execution replication.OnEventFunc) error {
	if parser, ok := m.files[name]; ok {
		return parser.ParseFile(name, offset, execution)
	}
	return fmt.Errorf("file not found in mock: %s", name)
}

// ============================================================
// Resume Position Tests
// ============================================================

// TestResumePosition_CommitEqualsResume tests case where no next GTID exists
// Resume position should equal commit position
func TestResumePosition_CommitEqualsResume(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	gtidEvent := createGTIDEvent(targetUUID, 50)

	xidEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000, // Commit position
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 123},
	}

	// No next GTID - end of file
	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent,
			xidEvent,
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Commit position
	if result.CommitPosition != 2000 {
		t.Errorf("Expected commit position 2000, got %d", result.CommitPosition)
	}

	// Resume position should equal commit position when no next GTID
	if result.ResumePosition != 2000 {
		t.Errorf("Expected resume position 2000 (equals commit), got %d", result.ResumePosition)
	}

	// No next GTID should be set
	if result.NextGTID != "" {
		t.Errorf("Expected empty next GTID, got %s", result.NextGTID)
	}
}

// TestResumePosition_CommitNotEqualsResume tests case where next GTID exists
// Resume position should be END_LOG_POS of next GTID (matching Kafka Connect)
func TestResumePosition_CommitNotEqualsResume(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// GTID in range
	gtidEvent := createGTIDEvent(targetUUID, 50)

	xidEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000, // Commit position
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 123},
	}

	// Next GTID outside range (GNO=200 > 100)
	nextGTIDEvent := createGTIDEvent(targetUUID, 200)
	nextGTIDEvent.Header.LogPos = 2100 // END_LOG_POS = Resume position

	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent,
			xidEvent,
			nextGTIDEvent, // Next GTID after commit
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Commit position
	if result.CommitPosition != 2000 {
		t.Errorf("Expected commit position 2000, got %d", result.CommitPosition)
	}

	// Resume position = END_LOG_POS of next GTID
	if result.ResumePosition != 2100 {
		t.Errorf("Expected resume position 2100 (next GTID LogPos), got %d", result.ResumePosition)
	}

	// Next GTID should be captured
	expectedNextGTID := fmt.Sprintf("%s:200", targetUUID)
	if result.NextGTID != expectedNextGTID {
		t.Errorf("Expected next GTID %s, got %s", expectedNextGTID, result.NextGTID)
	}
}

// TestResumePosition_HighestGNOInRange tests that we return the highest GNO in range
func TestResumePosition_HighestGNOInRange(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// Multiple GTIDs in range
	gtidEvent1 := createGTIDEvent(targetUUID, 10)
	gtidEvent1.Header.LogPos = 500
	xidEvent1 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    600,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 1},
	}

	gtidEvent2 := createGTIDEvent(targetUUID, 50) // Higher GNO
	gtidEvent2.Header.LogPos = 1500
	xidEvent2 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 2},
	}

	// Next GTID outside range
	nextGTIDEvent := createGTIDEvent(targetUUID, 200)
	nextGTIDEvent.Header.LogPos = 2500

	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent1, xidEvent1,
			gtidEvent2, xidEvent2, // Higher GNO - this should be returned
			nextGTIDEvent,
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Should return highest GNO (50)
	if result.GNO != 50 {
		t.Errorf("Expected GNO 50 (highest in range), got %d", result.GNO)
	}

	// Commit position should be from highest GNO transaction
	if result.CommitPosition != 2000 {
		t.Errorf("Expected commit position 2000, got %d", result.CommitPosition)
	}

	// Resume position = END_LOG_POS of next GTID
	if result.ResumePosition != 2500 {
		t.Errorf("Expected resume position 2500, got %d", result.ResumePosition)
	}

	// Start position from highest GNO
	expectedStartPos := uint32(1500 - 100) // LogPos - EventSize
	if result.Position != expectedStartPos {
		t.Errorf("Expected start position %d, got %d", expectedStartPos, result.Position)
	}
}

// TestResumePosition_QueryEventCommit tests COMMIT via QUERY_EVENT (non-InnoDB)
func TestResumePosition_QueryEventCommit(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	gtidEvent := createGTIDEvent(targetUUID, 50)

	// COMMIT via QUERY_EVENT (not XID_EVENT)
	commitQueryEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
			LogPos:    2000,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.QueryEvent{
			Query: []byte("COMMIT"),
		},
	}

	nextGTIDEvent := createGTIDEvent(targetUUID, 200)
	nextGTIDEvent.Header.LogPos = 2500

	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent,
			commitQueryEvent,
			nextGTIDEvent,
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Should recognize QUERY_EVENT COMMIT
	if result.CommitPosition != 2000 {
		t.Errorf("Expected commit position 2000, got %d", result.CommitPosition)
	}

	if result.ResumePosition != 2500 {
		t.Errorf("Expected resume position 2500, got %d", result.ResumePosition)
	}
}

// TestResumePosition_DatabaseFilter tests database filtering
func TestResumePosition_DatabaseFilter(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// Query event to set database context
	dbQueryEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
			LogPos:    100,
			EventSize: 50,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.QueryEvent{
			Schema: []byte("other_db"), // Different database
			Query:  []byte("BEGIN"),
		},
	}

	gtidEvent1 := createGTIDEvent(targetUUID, 10)
	xidEvent1 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    600,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 1},
	}

	// Set target database
	targetDbQueryEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.QUERY_EVENT,
			LogPos:    700,
			EventSize: 50,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.QueryEvent{
			Schema: []byte("target_db"),
			Query:  []byte("BEGIN"),
		},
	}

	gtidEvent2 := createGTIDEvent(targetUUID, 50)
	gtidEvent2.Header.LogPos = 800
	xidEvent2 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    1000,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 2},
	}

	mockParser := &MockBinlogParser{
		events: []interface{}{
			dbQueryEvent,
			gtidEvent1, xidEvent1, // Should be skipped (other_db)
			targetDbQueryEvent,
			gtidEvent2, xidEvent2, // Should match (target_db)
		},
	}

	searcher := &Searcher{
		config: &models.Config{
			FilterDatabase: "target_db",
		},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Should return GNO 50 (from target_db), not GNO 10 (from other_db)
	if result.GNO != 50 {
		t.Errorf("Expected GNO 50, got %d", result.GNO)
	}

	if result.Database != "target_db" {
		t.Errorf("Expected database target_db, got %s", result.Database)
	}
}

// TestResumePosition_StartPosition verifies start position calculation
func TestResumePosition_StartPosition(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	gtidEvent := createGTIDEvent(targetUUID, 50)
	gtidEvent.Header.LogPos = 1000    // END position
	gtidEvent.Header.EventSize = 100  // Size

	xidEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.XID_EVENT,
			LogPos:    2000,
			EventSize: 100,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.XIDEvent{XID: 123},
	}

	mockParser := &MockBinlogParser{
		events: []interface{}{
			gtidEvent,
			xidEvent,
		},
	}

	searcher := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser {
			return mockParser
		},
	}

	result, err := searcher.searchBinlogFile("test-file", &targetGTID)

	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if result == nil {
		t.Fatal("Expected result, got nil")
	}

	// Start position = LogPos - EventSize
	expectedStartPos := uint32(1000 - 100) // 900
	if result.Position != expectedStartPos {
		t.Errorf("Expected start position %d (LogPos - EventSize), got %d", expectedStartPos, result.Position)
	}
}
