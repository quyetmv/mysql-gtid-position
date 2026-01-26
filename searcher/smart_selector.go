package searcher

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/go-mysql-org/go-mysql/mysql"
)

// FindStartFileUsingHeaders finds the optimal start file using PREVIOUS_GTIDS headers.
// It iterates through files and checks their headers.
// It returns the filename of the first file where TargetGTID is NOT fully in the past.
func (s *Searcher) FindStartFileUsingHeaders(files []string, targetGTID *mysql.GTIDSet) (string, error) {
	if len(files) == 0 {
		return "", fmt.Errorf("no files to search")
	}

	if s.verbose {
		fmt.Printf("🧠 Smart Selecting Start File from %d files...\n", len(files))
	}

	// Binary search for the first file where Prev() Contains Target.
	// That index is 'idx'.
	// Then target file is 'idx - 1'.
	
	idx := sortSearch(len(files), func(i int) bool {
		// Check header of files[i]
		skipped, err := s.CheckPreviousGTIDs(files[i], targetGTID)
		if err != nil {
			// On error, we assume False (don't skip/not contained) to be safe?
			// Or just falback.
			fmt.Fprintf(os.Stderr, "Warning: Failed to check header of %s: %v\n", files[i], err)
			return false 
		}
		return skipped // True if Prev contains Target
	})
	
	// idx is the first index where Prev contains Target.
	// If idx == 0: Even 1st file says target is in past. (Target < Start of logs).
	// If idx == N: No file says target is in past. (Target > All logs).
	
	if idx == 0 {
		if s.verbose {
			fmt.Println("⚠️  Target seems to be before the first available binlog.")
		}
		return files[0], nil
	}
	
	if idx == len(files) {
		if s.verbose {
			fmt.Println("ℹ️  Target not found in Past of any file. Starting from last file.")
		}
		// Start from last file
		return files[len(files)-1], nil
	}
	
	// Found boundary. Target is in files[idx-1].
	bestFile := files[idx-1]
	if s.verbose {
		fmt.Printf("🎯 Smart Selection: Target found in range of %s\n", filepath.Base(bestFile))
	}
	
	return bestFile, nil
}

// Custom binary search wrapper
func sortSearch(n int, f func(int) bool) int {
	// Define Search(n) logic
	i, j := 0, n
	for i < j {
		h := int(uint(i+j) >> 1) // avoid overflow when computing h
		// i <= h < j
		if !f(h) {
			i = h + 1 // preserves f(i-1) == false
		} else {
			j = h // preserves f(j) == true
		}
	}
	// i == j, f(i-1) == false, and f(j) (= f(i)) == true  =>  answer is i.
	return i
}
