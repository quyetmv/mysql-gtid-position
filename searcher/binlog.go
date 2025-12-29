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
