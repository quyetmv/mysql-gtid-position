package exporter

import (
	"fmt"
	"strings"
	"time"

	"github.com/quyetmv/mysql-gtid-position/models"
)

// ConsoleExporter exports results to console with formatting
type ConsoleExporter struct {
	UseColor bool
}

// NewConsoleExporter creates a new console exporter
func NewConsoleExporter() *ConsoleExporter {
	return &ConsoleExporter{
		UseColor: true,
	}
}

// Export prints GTID positions to console
func (e *ConsoleExporter) Export(positions []*models.GTIDPosition, output string) error {
	if len(positions) == 0 {
		fmt.Println("❌ No GTID positions found")
		return nil
	}

	fmt.Println(strings.Repeat("=", 70))
	fmt.Printf("📊 Found %d GTID Position(s)\n", len(positions))
	fmt.Println(strings.Repeat("=", 70))

	for i, pos := range positions {
		fmt.Printf("\n[%d] GTID Position:\n", i+1)
		fmt.Println(strings.Repeat("-", 70))
		fmt.Printf("  📄 Binlog File: %s\n", pos.BinlogFile)
		fmt.Printf("  📍 Position:    %d\n", pos.Position)
		fmt.Printf("  🆔 GTID:        %s\n", pos.GTID)
		fmt.Printf("  🕐 Timestamp:   %s (%d)\n",
			time.Unix(int64(pos.Timestamp), 0).Format(time.RFC3339),
			pos.Timestamp)
	}

	fmt.Println(strings.Repeat("=", 70))
	return nil
}

// ExportSingle prints a single GTID position (for backward compatibility)
func (e *ConsoleExporter) ExportSingle(pos *models.GTIDPosition) error {
	if pos == nil {
		fmt.Println("❌ GTID not found")
		return nil
	}

	fmt.Println(strings.Repeat("-", 60))
	fmt.Println("✅ Found GTID")
	fmt.Printf("📄 Binlog File: %s\n", pos.BinlogFile)
	fmt.Printf("🆔 GTID: %s\n\n", pos.GTID)
	
	fmt.Printf("📍 Start Position (GTID):     %d\n", pos.Position)
	fmt.Printf("📍 Commit Position (Xid):     %d\n", pos.CommitPosition)
	fmt.Printf("📍 Resume Position:           %d   ✅\n", pos.ResumePosition)
	if pos.NextGTID != "" {
		fmt.Printf("🔄 Next GTID:                 %s\n", pos.NextGTID)
	}
	fmt.Println()
	
	fmt.Printf("🕐 Timestamp: %s\n",
		time.Unix(int64(pos.Timestamp), 0).Format(time.RFC3339))
	if pos.Database != "" {
		fmt.Printf("💾 Database: %s\n", pos.Database)
	}
	fmt.Println(strings.Repeat("-", 60))

	return nil
}
