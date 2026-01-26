package searcher

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/quyetmv/mysql-gtid-position/models"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

// BinlogParser interface matches replication.BinlogParser.ParseFile
type BinlogParser interface {
	ParseFile(name string, offset int64, execution replication.OnEventFunc) error
}

// Searcher handles binlog file searching
type Searcher struct {
	config        *models.Config
	verbose       bool
	parserFactory func() BinlogParser
}

// NewSearcher creates a new Searcher instance
func NewSearcher(config *models.Config) *Searcher {
	return &Searcher{
		config:  config,
		verbose: config.Verbose,
		parserFactory: func() BinlogParser {
			p := replication.NewBinlogParser()
			p.SetVerifyChecksum(true)
			return p
		},
	}
}

// GetBinlogFiles discovers binlog files in directory
func (s *Searcher) GetBinlogFiles(dir, pattern string) ([]string, error) {
	files, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil {
		return nil, fmt.Errorf("failed to glob files: %w", err)
	}

	// Filter out index files and sort
	var binlogs []string
	for _, f := range files {
		if !strings.HasSuffix(f, ".index") {
			binlogs = append(binlogs, f)
		}
	}

	sort.Strings(binlogs)
	return binlogs, nil
}

// SearchParallel searches for GTID in binlog files using parallel workers
func (s *Searcher) SearchParallel(files []string, targetGTID *mysql.GTIDSet) (*models.GTIDPosition, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	resultChan := make(chan *models.GTIDPosition, len(files))
	errorChan := make(chan error, len(files))

	var wg sync.WaitGroup
	semaphore := make(chan struct{}, s.config.Parallel)

	for i, file := range files {
		wg.Add(1)
		go func(idx int, filepath string) {
			defer wg.Done()

			semaphore <- struct{}{}
			defer func() { <-semaphore }()

			select {
			case <-ctx.Done():
				return
			default:
			}

			if s.verbose {
				fmt.Printf("🔎 Scanning [%d/%d]: %s\n", idx+1, len(files), filepath)
			}

			result, err := s.searchBinlogFile(filepath, targetGTID)
			if err != nil {
				errorChan <- fmt.Errorf("error scanning %s: %w", filepath, err)
				return
			}

			if result != nil {
				resultChan <- result
				cancel() // Stop other goroutines
			}
		}(i, file)
	}

	// Wait for all goroutines to complete
	go func() {
		wg.Wait()
		close(resultChan)
		close(errorChan)
	}()

	// Collect best result (highest GNO)
	var bestResult *models.GTIDPosition
	for result := range resultChan {
		if result != nil {
			if bestResult == nil || result.GNO > bestResult.GNO {
				bestResult = result
			}
		}
	}

	// Log any errors in verbose mode
	if s.verbose {
		for err := range errorChan {
			fmt.Fprintf(os.Stderr, "Warning: %v\n", err)
		}
	}

	return bestResult, nil
}


// searchBinlogFile searches for GTID in a single binlog file
func (s *Searcher) searchBinlogFile(filepath string, targetGTID *mysql.GTIDSet) (*models.GTIDPosition, error) {
	parser := s.parserFactory()

	var result *models.GTIDPosition
	var currentDatabase string // Track current database context
	var currentTransaction *models.GTIDPosition // Track current transaction being processed

	// Convert time filters to Unix timestamps for comparison
	var startTimestamp, endTimestamp uint32
	if !s.config.StartTime.IsZero() {
		startTimestamp = uint32(s.config.StartTime.Unix())
	}
	if !s.config.EndTime.IsZero() {
		endTimestamp = uint32(s.config.EndTime.Unix())
	}

	err := parser.ParseFile(filepath, 0, func(e *replication.BinlogEvent) error {
		// Filter by time range if specified
		if startTimestamp > 0 && e.Header.Timestamp < startTimestamp {
			return nil // Skip events before start time
		}
		if endTimestamp > 0 && e.Header.Timestamp > endTimestamp {
			return nil // Skip events after end time
		}

		// Track database context from QueryEvent
		if e.Header.EventType == replication.QUERY_EVENT {
			queryEvent := e.Event.(*replication.QueryEvent)
			if len(queryEvent.Schema) > 0 {
				currentDatabase = string(queryEvent.Schema)
			}
		}

		// Check for GTID event (start of transaction)
		if e.Header.EventType == replication.GTID_EVENT {
			gtidEvent := e.Event.(*replication.GTIDEvent)

			// Convert SID to UUID string
			uuidStr := fmt.Sprintf("%x-%x-%x-%x-%x",
				gtidEvent.SID[0:4], gtidEvent.SID[4:6], gtidEvent.SID[6:8],
				gtidEvent.SID[8:10], gtidEvent.SID[10:16])

			// Format GTID string
			gtidStr := fmt.Sprintf("%s:%d", uuidStr, gtidEvent.GNO)

			// Parse current GTID to check if it's in the target set
			currentGTID, err := mysql.ParseMysqlGTIDSet(gtidStr)
			if err != nil {
				return nil // Skip invalid GTIDs
			}

			// Check if current GTID is contained in target GTID set
			if (*targetGTID).Contain(currentGTID) {
				// Filter by database if specified
				if s.config.FilterDatabase != "" && currentDatabase != s.config.FilterDatabase {
					currentTransaction = nil
					return nil // Skip if database doesn't match
				}

				// Start tracking this transaction
				currentTransaction = &models.GTIDPosition{
					BinlogFile:     filepath,
					Position:       e.Header.LogPos - e.Header.EventSize, // Start position (GTID event)
					CommitPosition: e.Header.LogPos,                      // Will be updated at transaction end
					ResumePosition: e.Header.LogPos,                      // Will be updated when next GTID found
					Timestamp:      e.Header.Timestamp,
					GTID:           gtidStr,
					ServerUUID:     uuidStr,
					GNO:            uint64(gtidEvent.GNO),
					Database:       currentDatabase,
					CreatedAt:      time.Now(),
				}
			} else {
				// GTID outside target range
				// If we have completed result, this is the next GTID
				if result != nil && result.NextGTID == "" {
					result.NextGTID = gtidStr
					result.ResumePosition = e.Header.LogPos // END_LOG_POS of next GTID (same as Kafka Connect)
					return fmt.Errorf("found_next_gtid")
				}
				currentTransaction = nil
			}
		}

		// Track transaction end (XID_EVENT or COMMIT)
		if currentTransaction != nil {
			// XID_EVENT marks end of InnoDB transaction
			if e.Header.EventType == replication.XID_EVENT {
				// Update commit position (Xid END_LOG_POS) and timestamp
				currentTransaction.CommitPosition = e.Header.LogPos
				currentTransaction.ResumePosition = e.Header.LogPos // Default resume = commit
				currentTransaction.Timestamp = e.Header.Timestamp

				// Keep the match with highest GNO
				if result == nil || currentTransaction.GNO > result.GNO {
					result = currentTransaction
				}
				currentTransaction = nil
			}

			// QUERY_EVENT with COMMIT also marks transaction end
			if e.Header.EventType == replication.QUERY_EVENT {
				queryEvent := e.Event.(*replication.QueryEvent)
				query := string(queryEvent.Query)
				if query == "COMMIT" || query == "commit" {
					// Update commit position and timestamp
					currentTransaction.CommitPosition = e.Header.LogPos
					currentTransaction.ResumePosition = e.Header.LogPos // Default resume = commit
					currentTransaction.Timestamp = e.Header.Timestamp

					// Keep the match with highest GNO
					if result == nil || currentTransaction.GNO > result.GNO {
						result = currentTransaction
					}
					currentTransaction = nil
				}
			}
		}

		return nil
	})

	// Return the result (highest GNO found)
	if err != nil && err.Error() != "found_next_gtid" {
		return nil, err
	}

	return result, nil
}

// CheckPreviousGTIDs parses the binlog header to check PREVIOUS_GTIDS_LOG_EVENT.
// Returns true if the Target GTID has already been executed BEFORE this file.
// Returns error if parsing fails.
func (s *Searcher) CheckPreviousGTIDs(filepath string, targetGTID *mysql.GTIDSet) (bool, error) {
	parser := s.parserFactory()

	var previousGTIDs *mysql.GTIDSet
	foundHeader := false

	// We only need to scan the first few events (Header + PreviousGTIDs)
	// Usually it's within the first 1-2KB or first few events.
	err := parser.ParseFile(filepath, 0, func(e *replication.BinlogEvent) error {
		if e.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			prevGTIDsEvent := e.Event.(*replication.PreviousGTIDsEvent)
			gtidSet, err := mysql.ParseMysqlGTIDSet(prevGTIDsEvent.GTIDSets)
			if err != nil {
				return fmt.Errorf("failed to parse GTID set: %w", err)
			}
			previousGTIDs = &gtidSet
			foundHeader = true
			return fmt.Errorf("stop_scan") // Found it, stop scanning
		}
		
		// If we scanned too many events without finding PREVIOUS_GTIDS, verify if it exists?
		// Usually it is the second event after FormatDescriptionEvent.
		// Let's rely on finding it. If we don't find it quickly (e.g. log pos > 10000), abort?
		// For now, continue until found or EOF.
		// Note from experience: PreviousGTIDs is ALWAYS at the beginning.
		return nil
	})

	if err != nil && err.Error() != "stop_scan" {
		// If file is empty or other error
		return false, err
	}

	if !foundHeader || previousGTIDs == nil {
		// Maybe an old binlog without PreviousGTIDs?
		// In that case we can't skip safely, so assume false (don't skip).
		if s.verbose {
			fmt.Printf("⚠️  No PREVIOUS_GTIDS event found in %s\n", filepath)
		}
		return false, nil
	}

	// Logic:
	// If PreviousGTIDs CONTAINS TargetGTID => Target was executed BEFORE this file.
	// We assume TargetGTID in this context means "The specific GTID we are looking for".
	// But targetGTID is a Set (e.g. UUID:1-100).
	// We are looking for "Any GTID in TargetGTID set".
	// Wait, user inputs "UUID:1-100", usually searching for the LAST executed transaction in that set?
	// Or searching for a specific transaction?
	// Usually user gives "UUID:1-50" meaning "I have data up to 50, I want to find where 50 happened".
	// Or "I want to resume from 50".
	// The tool finds the "highest GNO in range".
	// Let's say user wants UUID:100.
	// If PreviousGTIDs has UUID:1-150 -> UUID:100 is in PAST files. Return TRUE (Skipped).
	// If PreviousGTIDs has UUID:1-50 -> UUID:100 is in FUTURE (or this) files. Return FALSE (Don't skip).
	
	// We need to check if ALL GTIDs in targetGTID are contained in previousGTIDs?
	// No, checking if *At least one* is present?
	// Actually, if we are looking for UUID:100.
	// We want to know if UUID:100 exists in this file or later.
	// IF UUID:100 is ALREADY in PreviousGTIDs, then it is NOT in this file (it was before).
	// So we can SKIP this file if PreviousGTIDs contains TargetGTID.
	
	// HOWEVER, "targetGTID" passed to this func is a Set.
	// If user passed UUID:1-100.
	// PreviousGTIDs: UUID:1-50.
	// Contain? No. (1-100 is not subset of 1-50).
	// PreviousGTIDs: UUID:1-200.
	// Contain? Yes. (1-100 is subset of 1-200).
	// So if Contain() is true, then ALL target GTIDs are in the past. We can skip this file. YES.
	
	if (*previousGTIDs).Contain(*targetGTID) {
		return true, nil // Target is in the past
	}

	return false, nil
}
