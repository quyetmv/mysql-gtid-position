package searcher

import (
	"context"
	"fmt"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/quyetmv/mysql-gtid-position/models"
)

// RemoteSearcher handles searching GTID via MySQL connection
type RemoteSearcher struct {
	config *models.Config
}

// NewRemoteSearcher creates a new RemoteSearcher instance
func NewRemoteSearcher(config *models.Config) *RemoteSearcher {
	return &RemoteSearcher{
		config: config,
	}
}

// Search connects to MySQL and searches for the target GTID
func (s *RemoteSearcher) Search(targetGTID *mysql.GTIDSet) (*models.GTIDPosition, error) {
	// Create binlog syncer
	cfg := replication.BinlogSyncerConfig{
		ServerID: 100, // Random server ID
		Flavor:   "mysql",
		Host:     s.config.Host,
		Port:     uint16(s.config.Port),
		User:     s.config.User,
		Password: s.config.Password,
	}
	syncer := replication.NewBinlogSyncer(cfg)
	defer syncer.Close()

	// Connect to MySQL
	startFile := s.config.StartFile
	startPos := uint32(4)

	if startFile == "" {
		return nil, fmt.Errorf("remote search currently requires -start-file to be specified")
	}

	streamer, err := syncer.StartSync(mysql.Position{Name: startFile, Pos: startPos})
	if err != nil {
		return nil, fmt.Errorf("failed to start sync: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var result *models.GTIDPosition
	var currentDatabase string
	var currentTransaction *models.GTIDPosition

	// Filters
	var startTimestamp, endTimestamp uint32
	if !s.config.StartTime.IsZero() {
		startTimestamp = uint32(s.config.StartTime.Unix())
	}
	if !s.config.EndTime.IsZero() {
		endTimestamp = uint32(s.config.EndTime.Unix())
	}

	if s.config.Verbose {
		fmt.Printf("📡 Connected to %s:%d, streaming from %s:%d\n", s.config.Host, s.config.Port, startFile, startPos)
	}

	for {
		// Use a timeout for GetEvent to allow efficient cancellation or stopping
		// Using a context with timeout for GetEvent
		timeoutCtx, timeoutCancel := context.WithTimeout(ctx, 30*time.Second) // 30s timeout if no events?
		ev, err := streamer.GetEvent(timeoutCtx)
		timeoutCancel()
		
		if err != nil {
			if err == context.DeadlineExceeded {
				// No events for 30s, assume we reached end or stalled
				if result != nil {
					return result, nil
				}
				// Or continue? For now let's stop if we are searching for past events
				// If FindAll is true, we might want to keep waiting? 
				// But this is a "Search" tool, not a daemon. 
				// If no events flow, we likely caught up or network issue.
				return result, nil 
			}
			return nil, fmt.Errorf("get event error: %w", err)
		}

		// Process Event
		
		// Filter by time
		if startTimestamp > 0 && ev.Header.Timestamp < startTimestamp {
			continue
		}
		if endTimestamp > 0 && ev.Header.Timestamp > endTimestamp {
			if s.config.Verbose {
				fmt.Println("⏰ Reached end-time limit, stopping search.")
			}
			return result, nil
		}

		// Track Database
		if ev.Header.EventType == replication.QUERY_EVENT {
			queryEvent := ev.Event.(*replication.QueryEvent)
			if len(queryEvent.Schema) > 0 {
				currentDatabase = string(queryEvent.Schema)
			}
		}

		// Check GTID
		if ev.Header.EventType == replication.GTID_EVENT {
			gtidEvent := ev.Event.(*replication.GTIDEvent)
			uuidStr := fmt.Sprintf("%x-%x-%x-%x-%x",
				gtidEvent.SID[0:4], gtidEvent.SID[4:6], gtidEvent.SID[6:8],
				gtidEvent.SID[8:10], gtidEvent.SID[10:16])
			gtidStr := fmt.Sprintf("%s:%d", uuidStr, gtidEvent.GNO)

			currentGTID, err := mysql.ParseMysqlGTIDSet(gtidStr)
			if err != nil {
				continue
			}

			if (*targetGTID).Contain(currentGTID) {
				// Filter Database
				if s.config.FilterDatabase != "" && currentDatabase != s.config.FilterDatabase {
					currentTransaction = nil
					continue
				}

				// Found match
				currentTransaction = &models.GTIDPosition{
					BinlogFile:     startFile,
					Position:       ev.Header.LogPos - ev.Header.EventSize, 
					CommitPosition: ev.Header.LogPos,
					ResumePosition: ev.Header.LogPos,
					Timestamp:      ev.Header.Timestamp,
					GTID:           gtidStr,
					ServerUUID:     uuidStr,
					GNO:            uint64(gtidEvent.GNO),
					Database:       currentDatabase,
					CreatedAt:      time.Now(),
				}
			} else {
				// GTID outside target range
				// If we have a result, this is the Next GTID (Resume Position)
				if result != nil && result.NextGTID == "" {
					result.NextGTID = gtidStr
					result.ResumePosition = ev.Header.LogPos
					return result, nil
				}
				currentTransaction = nil
			}
		}

		// Handle Rotation to update filename
		if ev.Header.EventType == replication.ROTATE_EVENT {
			rotateEvent := ev.Event.(*replication.RotateEvent)
			startFile = string(rotateEvent.NextLogName)
			if s.config.Verbose {
				fmt.Printf("🔄 Rotated to: %s\n", startFile)
			}
		}

		// Transaction End
		if currentTransaction != nil {
			currentTransaction.BinlogFile = startFile

			if ev.Header.EventType == replication.XID_EVENT {
				currentTransaction.CommitPosition = ev.Header.LogPos
				currentTransaction.ResumePosition = ev.Header.LogPos
				currentTransaction.Timestamp = ev.Header.Timestamp
				
				if result == nil || currentTransaction.GNO > result.GNO {
					result = currentTransaction
				}
				currentTransaction = nil
			}

			if ev.Header.EventType == replication.QUERY_EVENT {
				queryEvent := ev.Event.(*replication.QueryEvent)
				query := string(queryEvent.Query)
				if query == "COMMIT" || query == "commit" {
					currentTransaction.CommitPosition = ev.Header.LogPos
					currentTransaction.ResumePosition = ev.Header.LogPos
					currentTransaction.Timestamp = ev.Header.Timestamp

					if result == nil || currentTransaction.GNO > result.GNO {
						result = currentTransaction
					}
					currentTransaction = nil
				}
			}
		}
	}
}
