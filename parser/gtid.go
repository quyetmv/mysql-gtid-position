package parser

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// ParseGTID parses a GTID string into GTIDSet
// Supports MySQL GTID format: server_uuid:transaction_id
// Example: 3E11FA47-71CA-11E1-9E33-C80AA9429562:23
func ParseGTID(gtidStr string) (mysql.GTIDSet, error) {
	if gtidStr == "" {
		return nil, fmt.Errorf("GTID string cannot be empty")
	}

	gtidStr = strings.TrimSpace(gtidStr)
	
	gtidSet, err := mysql.ParseMysqlGTIDSet(gtidStr)
	if err != nil {
		return nil, fmt.Errorf("invalid GTID format '%s': %w", gtidStr, err)
	}
	
	return gtidSet, nil
}

// ParseGTIDFile reads GTIDs from a file (one per line)
// Returns a slice of GTIDSet for batch processing
func ParseGTIDFile(filepath string) ([]mysql.GTIDSet, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open GTID file: %w", err)
	}
	defer file.Close()

	var gtidSets []mysql.GTIDSet
	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		
		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		gtidSet, err := ParseGTID(line)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", lineNum, err)
		}
		
		gtidSets = append(gtidSets, gtidSet)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	if len(gtidSets) == 0 {
		return nil, fmt.Errorf("no valid GTIDs found in file")
	}

	return gtidSets, nil
}

// ValidateGTIDFormat checks if a string matches GTID format
// without fully parsing it (lightweight validation)
func ValidateGTIDFormat(gtidStr string) error {
	gtidStr = strings.TrimSpace(gtidStr)
	
	if gtidStr == "" {
		return fmt.Errorf("GTID cannot be empty")
	}

	// Basic format check: UUID:number
	parts := strings.Split(gtidStr, ":")
	if len(parts) != 2 {
		return fmt.Errorf("GTID must be in format 'UUID:transaction_id'")
	}

	// UUID should be 36 characters (with hyphens)
	uuid := parts[0]
	if len(uuid) != 36 {
		return fmt.Errorf("invalid UUID length: expected 36, got %d", len(uuid))
	}

	return nil
}

// UUIDInfo contains information about a UUID in a GTID set
type UUIDInfo struct {
	UUID           string
	MaxTransaction uint64
	MinTransaction uint64
	TotalCount     uint64
}

// ExtractUUIDs extracts all UUIDs from a GTID set with their transaction info
func ExtractUUIDs(gtidSet *mysql.GTIDSet) ([]UUIDInfo, error) {
	if gtidSet == nil {
		return nil, fmt.Errorf("GTID set cannot be nil")
	}

	// Get the underlying MysqlGTIDSet
	mysqlSet, ok := (*gtidSet).(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("expected MysqlGTIDSet type")
	}

	var uuidInfos []UUIDInfo
	
	// Iterate through all UUIDs in the set
	for uuid, intervals := range mysqlSet.Sets {
		if len(intervals.Intervals) == 0 {
			continue
		}

		info := UUIDInfo{
			UUID:           uuid,
			MinTransaction: uint64(intervals.Intervals[0].Start),
			MaxTransaction: uint64(intervals.Intervals[len(intervals.Intervals)-1].Stop - 1),
		}

		// Calculate total transaction count
		var total uint64
		for _, interval := range intervals.Intervals {
			total += uint64(interval.Stop - interval.Start)
		}
		info.TotalCount = total

		uuidInfos = append(uuidInfos, info)
	}

	return uuidInfos, nil
}

// FindActiveMasterUUID finds the UUID with the highest transaction number
// This is typically the current/active master in a multi-master setup
func FindActiveMasterUUID(gtidSet *mysql.GTIDSet) (string, error) {
	uuidInfos, err := ExtractUUIDs(gtidSet)
	if err != nil {
		return "", err
	}

	if len(uuidInfos) == 0 {
		return "", fmt.Errorf("no UUIDs found in GTID set")
	}

	// Find UUID with highest max transaction number
	activeMaster := uuidInfos[0]
	for _, info := range uuidInfos[1:] {
		if info.MaxTransaction > activeMaster.MaxTransaction {
			activeMaster = info
		}
	}

	return activeMaster.UUID, nil
}

// FilterByUUID creates a new GTID set containing only the specified UUID
func FilterByUUID(gtidSet *mysql.GTIDSet, targetUUID string) (mysql.GTIDSet, error) {
	if gtidSet == nil {
		return nil, fmt.Errorf("GTID set cannot be nil")
	}

	// Get the underlying MysqlGTIDSet
	mysqlSet, ok := (*gtidSet).(*mysql.MysqlGTIDSet)
	if !ok {
		return nil, fmt.Errorf("expected MysqlGTIDSet type")
	}

	// Find the target UUID in the set
	for uuid, intervals := range mysqlSet.Sets {
		if uuid == targetUUID {
			// Create a new GTID set with only this UUID
			newSet := &mysql.MysqlGTIDSet{
				Sets: map[string]*mysql.UUIDSet{
					uuid: intervals,
				},
			}
			return newSet, nil
		}
	}

	return nil, fmt.Errorf("UUID %s not found in GTID set", targetUUID)
}

// ExtractGTIDInfo extracts UUID and GNO from a GTID string
// Example: "3E11FA47-71CA-11E1-9E33-C80AA9429562:23" -> ("3E11FA47-71CA-11E1-9E33-C80AA9429562", 23)
func ExtractGTIDInfo(gtidStr string) (uuid string, gno uint64, err error) {
	parts := strings.Split(gtidStr, ":")
	if len(parts) != 2 {
		return "", 0, fmt.Errorf("invalid GTID format: %s", gtidStr)
	}

	uuid = parts[0]
	
	// Parse GNO
	_, err = fmt.Sscanf(parts[1], "%d", &gno)
	if err != nil {
		return "", 0, fmt.Errorf("invalid GNO in GTID: %w", err)
	}

	return uuid, gno, nil
}
