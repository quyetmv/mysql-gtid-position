package models

import "time"

// GTIDPosition represents the location of a GTID in a binlog file
type GTIDPosition struct {
	BinlogFile     string    `json:"binlog_file" csv:"binlog_file"`
	Position       uint32    `json:"start_position" csv:"start_position"`         // Start position (GTID event)
	CommitPosition uint32    `json:"commit_position" csv:"commit_position"`       // Commit position (Xid END_LOG_POS)
	ResumePosition uint32    `json:"resume_position" csv:"resume_position"`       // Resume position (END_LOG_POS of next GTID)
	Timestamp      uint32    `json:"timestamp" csv:"timestamp"`
	GTID           string    `json:"gtid" csv:"gtid"`
	ServerUUID     string    `json:"server_uuid" csv:"server_uuid"`
	GNO            uint64    `json:"gno" csv:"gno"`
	Database       string    `json:"database,omitempty" csv:"database"`
	NextGTID       string    `json:"next_gtid,omitempty" csv:"next_gtid"` // Next GTID for debug
	CreatedAt      time.Time `json:"created_at,omitempty" csv:"-"`
}

// TimestampReadable returns human-readable timestamp
func (g *GTIDPosition) TimestampReadable() string {
	return time.Unix(int64(g.Timestamp), 0).Format(time.RFC3339)
}

// Config holds application configuration
type Config struct {
	BinlogDir        string
	TargetGTID       string
	GTIDFile         string // File containing multiple GTIDs for batch mode
	FilePattern      string
	StartFile        string    // Start searching from this binlog file (e.g., mysql-bin.000100)
	Parallel         int
	Verbose          bool
	OutputFormat     ExportFormat
	OutputFile       string
	FindActiveMaster bool      // Auto-detect and search for active master UUID (highest GNO)
	FilterUUID       string    // Filter search by specific server UUID
	FilterDatabase   string    // Filter search by database name
	StartTime        time.Time // Filter events after this time
	EndTime          time.Time // Filter events before this time
	FindAll          bool      // Find all GTIDs in range (not just first match)
	Host             string    // MySQL Host
	Port             int       // MySQL Port
	User             string    // MySQL User
	Password         string    // MySQL Password
}

// ExportFormat represents output format type
type ExportFormat string

const (
	FormatConsole ExportFormat = "console"
	FormatCSV     ExportFormat = "csv"
	FormatJSON    ExportFormat = "json"
)

// SearchResult contains search results with metadata
type SearchResult struct {
	Positions     []*GTIDPosition `json:"positions"`
	TotalFiles    int             `json:"total_files"`
	ScannedFiles  int             `json:"scanned_files"`
	Duration      time.Duration   `json:"duration"`
	Error         error           `json:"error,omitempty"`
}

// IsValid checks if export format is valid
func (f ExportFormat) IsValid() bool {
	switch f {
	case FormatConsole, FormatCSV, FormatJSON:
		return true
	default:
		return false
	}
}
