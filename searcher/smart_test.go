package searcher

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/quyetmv/mysql-gtid-position/models"
)

// Tests for Smart File Selection logic
// Place this in a new file: searcher/smart_test.go

func TestCheckPreviousGTIDs(t *testing.T) {
	// Setup
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)

	// Case 1: Previous GTIDs contains Target (Executed in past)
	// Previous: UUID:1-200 (Superset of 1-100)
	prevGTIDsStr := fmt.Sprintf("%s:1-200", targetUUID)
	
	prevEvent := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.PREVIOUS_GTIDS_EVENT,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.PreviousGTIDsEvent{
			GTIDSets: prevGTIDsStr,
		},
	}
	
	mockParserPast := &MockBinlogParser{
		events: []interface{}{prevEvent},
	}

	searcherPast := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser { return mockParserPast },
	}

	skipped, err := searcherPast.CheckPreviousGTIDs("dummy", &targetGTID)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if !skipped {
		t.Error("Expected to skip file (Target is in Past), got false")
	}

	// Case 2: Previous GTIDs does NOT contain Target (Not fully executed)
	// Previous: UUID:1-50 (Subset of 1-100)
	prevGTIDsStr2 := fmt.Sprintf("%s:1-50", targetUUID)
	
	prevEvent2 := &replication.BinlogEvent{
		Header: &replication.EventHeader{
			EventType: replication.PREVIOUS_GTIDS_EVENT,
			Timestamp: uint32(time.Now().Unix()),
		},
		Event: &replication.PreviousGTIDsEvent{
			GTIDSets: prevGTIDsStr2,
		},
	}
	
	mockParserFuture := &MockBinlogParser{
		events: []interface{}{prevEvent2},
	}

	searcherFuture := &Searcher{
		config: &models.Config{},
		parserFactory: func() BinlogParser { return mockParserFuture },
	}

	skipped2, err := searcherFuture.CheckPreviousGTIDs("dummy", &targetGTID)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if skipped2 {
		t.Error("Expected NOT to skip file (Target is in Future/Current), got true")
	}
}

func TestFindStartFileUsingHeaders(t *testing.T) {
	targetUUID := "3e11fa47-71ca-11e1-9e33-c80aa9429562"
	targetGTIDStr := fmt.Sprintf("%s:1-100", targetUUID)
	targetGTID, _ := mysql.ParseMysqlGTIDSet(targetGTIDStr)
	
	// Files:
	// File 1: Prev=1-50  (Contains 1-100? No)
	// File 2: Prev=1-80  (Contains 1-100? No)
	// File 3: Prev=1-120 (Contains 1-100? Yes) -> Skip
	
	// Expect FindStartFile to return File 2.
	
	file1Evt := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.PREVIOUS_GTIDS_EVENT},
		Event: &replication.PreviousGTIDsEvent{GTIDSets: fmt.Sprintf("%s:1-50", targetUUID)},
	}
	file2Evt := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.PREVIOUS_GTIDS_EVENT},
		Event: &replication.PreviousGTIDsEvent{GTIDSets: fmt.Sprintf("%s:1-80", targetUUID)},
	}
	file3Evt := &replication.BinlogEvent{
		Header: &replication.EventHeader{EventType: replication.PREVIOUS_GTIDS_EVENT},
		Event: &replication.PreviousGTIDsEvent{GTIDSets: fmt.Sprintf("%s:1-120", targetUUID)},
	}

	mockMap := map[string]*MockBinlogParser{
		"bin.001": {events: []interface{}{file1Evt}},
		"bin.002": {events: []interface{}{file2Evt}},
		"bin.003": {events: []interface{}{file3Evt}},
	}
	
	smartMock := &SmartMockParser{files: mockMap}
	
	searcher := &Searcher{
		config: &models.Config{Verbose: true},
		parserFactory: func() BinlogParser { return smartMock },
	}
	
	files := []string{"bin.001", "bin.002", "bin.003"}
	
	startFile, err := searcher.FindStartFileUsingHeaders(files, &targetGTID)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	
	if startFile != "bin.002" {
		t.Errorf("Expected Start File bin.002, got %s", startFile)
	}
}

// Need to redefine SmartMockParser here if running as separate test file
// But since it's in same package `searcher`, it shares definitions if we run `go test ./searcher`

// However, if we put this in smart_test.go, it might not see internal structs of binlog_test.go unless exported.
// Note: test files in same package share visibility. `SmartMockParser` is defined in `binlog_test.go`.
