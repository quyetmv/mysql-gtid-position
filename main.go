package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/quyetmv/mysql-gtid-position/exporter"
	"github.com/quyetmv/mysql-gtid-position/models"
	"github.com/quyetmv/mysql-gtid-position/parser"
	"github.com/quyetmv/mysql-gtid-position/searcher"
)

func main() {
	cfg := parseFlags()

	if err := validateConfig(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	start := time.Now()
	fmt.Printf("🔍 Searching for GTID: %s\n", cfg.TargetGTID)
	fmt.Printf("📂 Binlog directory: %s\n", cfg.BinlogDir)
	fmt.Printf("📊 Output format: %s\n", cfg.OutputFormat)
	fmt.Println(strings.Repeat("-", 60))

	result, err := findGTIDPosition(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "❌ Error: %v\n", err)
		os.Exit(1)
	}

	if result == nil {
		fmt.Println("❌ GTID not found in binlog files")
		os.Exit(1)
	}

	elapsed := time.Since(start)
	
	// Export result based on format
	if err := exportResult(result, cfg, elapsed); err != nil {
		fmt.Fprintf(os.Stderr, "❌ Export error: %v\n", err)
		os.Exit(1)
	}
}

func parseFlags() *models.Config {
	cfg := &models.Config{}

	var formatStr string
	var startTimeStr, endTimeStr string

	flag.StringVar(&cfg.BinlogDir, "dir", "", "Binlog directory path (required)")
	flag.StringVar(&cfg.TargetGTID, "gtid", "", "Target GTID to find (required)")
	flag.StringVar(&cfg.GTIDFile, "gtid-file", "", "File containing multiple GTIDs (one per line)")
	flag.StringVar(&cfg.FilePattern, "pattern", "mysql-bin.*", "Binlog file pattern")
	flag.StringVar(&cfg.StartFile, "start-file", "", "Start searching from this binlog file (e.g., mysql-bin.000100)")
	flag.IntVar(&cfg.Parallel, "parallel", 4, "Number of parallel workers")
	flag.BoolVar(&cfg.Verbose, "verbose", false, "Verbose output")
	flag.StringVar(&formatStr, "format", "console", "Output format: console, csv, json")
	flag.StringVar(&cfg.OutputFile, "output", "", "Output file (default: stdout)")
	flag.BoolVar(&cfg.FindActiveMaster, "find-active-master", false, "Auto-detect and search for active master UUID (highest GNO)")
	flag.StringVar(&cfg.FilterUUID, "uuid", "", "Filter search by specific server UUID")
	flag.StringVar(&cfg.FilterDatabase, "database", "", "Filter search by database name")
	flag.StringVar(&startTimeStr, "start-time", "", "Filter events after this time (format: 2006-01-02 15:04:05 or RFC3339)")
	flag.StringVar(&endTimeStr, "end-time", "", "Filter events before this time (format: 2006-01-02 15:04:05 or RFC3339)")
	flag.BoolVar(&cfg.FindAll, "find-all", false, "Find all GTIDs in range (not just first match)")
	flag.StringVar(&cfg.Host, "host", "", "MySQL Host")
	flag.IntVar(&cfg.Port, "port", 3306, "MySQL Port")
	flag.StringVar(&cfg.User, "user", "", "MySQL User")
	flag.StringVar(&cfg.Password, "password", "", "MySQL Password")

	flag.Parse()

	// Parse format
	cfg.OutputFormat = models.ExportFormat(formatStr)

	// Parse time filters
	if startTimeStr != "" {
		if t, err := parseTimeString(startTimeStr); err == nil {
			cfg.StartTime = t
		} else {
			fmt.Fprintf(os.Stderr, "Warning: Invalid start-time format: %v\n", err)
		}
	}
	if endTimeStr != "" {
		if t, err := parseTimeString(endTimeStr); err == nil {
			cfg.EndTime = t
		} else {
			fmt.Fprintf(os.Stderr, "Warning: Invalid end-time format: %v\n", err)
		}
	}

	return cfg
}

func validateConfig(cfg *models.Config) error {
	if cfg.BinlogDir == "" && cfg.Host == "" {
		return fmt.Errorf("either binlog directory (-dir) or mysql host (-host) is required")
	}
	if cfg.BinlogDir != "" && cfg.Host != "" {
		return fmt.Errorf("cannot specify both -dir and -host")
	}
	if cfg.Host != "" && (cfg.User == "" || cfg.Password == "") {
		return fmt.Errorf("user and password are required when using -host")
	}
	if cfg.TargetGTID == "" && cfg.GTIDFile == "" {
		return fmt.Errorf("either -gtid or -gtid-file is required")
	}
	if cfg.TargetGTID != "" && cfg.GTIDFile != "" {
		return fmt.Errorf("cannot specify both -gtid and -gtid-file")
	}
	if _, err := os.Stat(cfg.BinlogDir); os.IsNotExist(err) {
		return fmt.Errorf("binlog directory does not exist: %s", cfg.BinlogDir)
	}
	if cfg.Host != "" && cfg.FindActiveMaster {
		return fmt.Errorf("-find-active-master is currently supported only for local binlog files (-dir)")
	}
	if cfg.Host != "" && cfg.StartFile == "" {
		return fmt.Errorf("-start-file is required when using -host")
	}
	if !cfg.OutputFormat.IsValid() {
		return fmt.Errorf("invalid output format: %s (must be console, csv, or json)", cfg.OutputFormat)
	}
	return nil
}

func findGTIDPosition(cfg *models.Config) (*models.GTIDPosition, error) {
	// Parse target GTID
	targetGTID, err := parser.ParseGTID(cfg.TargetGTID)
	if err != nil {
		return nil, fmt.Errorf("invalid GTID format: %v", err)
	}

	// Remote Search
	if cfg.Host != "" {
		if cfg.Verbose {
			fmt.Printf("🚀 Starting remote search on %s:%d\n", cfg.Host, cfg.Port)
		}
		s := searcher.NewRemoteSearcher(cfg)
		return s.Search(&targetGTID)
	}

	// Local Search
	// Create searcher
	s := searcher.NewSearcher(cfg)

	// Get all binlog files
	binlogFiles, err := s.GetBinlogFiles(cfg.BinlogDir, cfg.FilePattern)
	if err != nil {
		return nil, err
	}

	if len(binlogFiles) == 0 {
		return nil, fmt.Errorf("no binlog files found")
	}

	// Smart File Selection:
	// If StartFile is NOT specified, try to find the best start file using PreviousGTIDs headers
	if cfg.StartFile == "" {
		// Only if we found files
		if len(binlogFiles) > 0 {
			// Parse target GTID (needed for check)
			targetGTIDToCheck, err := parser.ParseGTID(cfg.TargetGTID)
			if err == nil {
				startFile, err := s.FindStartFileUsingHeaders(binlogFiles, &targetGTIDToCheck)
				if err == nil && startFile != "" {
					cfg.StartFile = filepath.Base(startFile)
				}
			}
		}
	}

	// Filter binlog files if start-file is specified (or auto-detected)
	if cfg.StartFile != "" {
		var filteredFiles []string
		startFound := false
		for _, file := range binlogFiles {
			// Check if this is the start file or we've already found it
			if !startFound {
				if strings.HasSuffix(file, cfg.StartFile) || filepath.Base(file) == cfg.StartFile {
					startFound = true
				} else {
					continue // Skip files before start-file
				}
			}
			filteredFiles = append(filteredFiles, file)
		}
		
		if !startFound {
			return nil, fmt.Errorf("start file '%s' not found in binlog files", cfg.StartFile)
		}
		
		binlogFiles = filteredFiles
		if cfg.Verbose {
			fmt.Printf("📂 Starting from file: %s (%d files to scan)\n", cfg.StartFile, len(binlogFiles))
		}
	}

	fmt.Printf("📋 Found %d binlog files\n", len(binlogFiles))

	// Handle active master detection (Local only)
	if cfg.FindActiveMaster {
		activeMasterUUID, err := parser.FindActiveMasterUUID(&targetGTID)
		if err != nil {
			return nil, fmt.Errorf("failed to find active master: %v", err)
		}
		fmt.Printf("🎯 Active master UUID detected: %s\n", activeMasterUUID)
		cfg.FilterUUID = activeMasterUUID
	}

	// Filter by UUID if specified
	if cfg.FilterUUID != "" {
		fmt.Printf("🔍 Filtering by UUID: %s\n", cfg.FilterUUID)
		targetGTID, err = parser.FilterByUUID(&targetGTID, cfg.FilterUUID)
		if err != nil {
			return nil, fmt.Errorf("failed to filter by UUID: %v", err)
		}
	}

	// Show GTID info if verbose
	if cfg.Verbose {
		uuidInfos, _ := parser.ExtractUUIDs(&targetGTID)
		fmt.Println("\n📊 GTID Set Information:")
		for _, info := range uuidInfos {
			fmt.Printf("  UUID: %s\n", info.UUID)
			fmt.Printf("    Transactions: %d-%d (total: %d)\n", 
				info.MinTransaction, info.MaxTransaction, info.TotalCount)
		}
		fmt.Println()
	}

	// Search in parallel
	return s.SearchParallel(binlogFiles, &targetGTID)
}

// parseTimeString parses time string in multiple formats
func parseTimeString(timeStr string) (time.Time, error) {
	// Try RFC3339 format first
	if t, err := time.Parse(time.RFC3339, timeStr); err == nil {
		return t, nil
	}
	
	// Try common format: 2006-01-02 15:04:05
	if t, err := time.Parse("2006-01-02 15:04:05", timeStr); err == nil {
		return t, nil
	}
	
	// Try date only: 2006-01-02
	if t, err := time.Parse("2006-01-02", timeStr); err == nil {
		return t, nil
	}
	
	return time.Time{}, fmt.Errorf("invalid time format, use: 2006-01-02 15:04:05 or RFC3339")
}

func exportResult(result *models.GTIDPosition, cfg *models.Config, elapsed time.Duration) error {
	// Print search summary for non-console formats
	if cfg.OutputFormat != models.FormatConsole {
		fmt.Println(strings.Repeat("-", 60))
		fmt.Printf("✅ Found GTID in %.2f seconds\n", elapsed.Seconds())
		fmt.Println(strings.Repeat("-", 60))
	}

	positions := []*models.GTIDPosition{result}

	switch cfg.OutputFormat {
	case models.FormatCSV:
		exp := exporter.NewCSVExporter()
		return exp.Export(positions, cfg.OutputFile)

	case models.FormatJSON:
		exp := exporter.NewJSONExporter(true)
		return exp.Export(positions, cfg.OutputFile)

	case models.FormatConsole:
		fmt.Println(strings.Repeat("-", 60))
		fmt.Printf("✅ Found GTID in %.2f seconds\n\n", elapsed.Seconds())
		exp := exporter.NewConsoleExporter()
		return exp.ExportSingle(result)

	default:
		return fmt.Errorf("unsupported output format: %s", cfg.OutputFormat)
	}
}