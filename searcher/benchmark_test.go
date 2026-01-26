package searcher

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/quyetmv/mysql-gtid-position/models"
)

// TestComparePerformance benchmarks Local vs Remote search
// Run with:
// BENCHMARK_ENABLE=true \
// BENCHMARK_HOST=127.0.0.1 BENCHMARK_PORT=3306 BENCHMARK_USER=root BENCHMARK_PASS=root \
// BENCHMARK_DIR=/data/log BENCHMARK_FILE=mysql-bin.000004 \
// BENCHMARK_GTID="..." \
// go test -v ./searcher -run TestComparePerformance
func TestComparePerformance(t *testing.T) {
	if os.Getenv("BENCHMARK_ENABLE") != "true" {
		t.Skip("Skipping benchmark test. Set BENCHMARK_ENABLE=true to run.")
	}

	// Load Config from Env
	host := os.Getenv("BENCHMARK_HOST")
	portStr := os.Getenv("BENCHMARK_PORT")
	user := os.Getenv("BENCHMARK_USER")
	pass := os.Getenv("BENCHMARK_PASS")
	dir := os.Getenv("BENCHMARK_DIR")
	startFile := os.Getenv("BENCHMARK_FILE")
	gtid := os.Getenv("BENCHMARK_GTID")

	if host == "" || dir == "" || gtid == "" {
		t.Fatal("Missing required env vars: BENCHMARK_HOST, BENCHMARK_DIR, BENCHMARK_GTID")
	}

	port, _ := strconv.Atoi(portStr)
	if port == 0 {
		port = 3306
	}

	fmt.Println("\n🚀 Starting Performance Benchmark")
	fmt.Println("==================================================")
	fmt.Printf("GTID: %s\n", gtid)
	fmt.Printf("File: %s\n", startFile)
	fmt.Println("==================================================")

	targetGTID, err := mysql.ParseMysqlGTIDSet(gtid)
	if err != nil {
		t.Fatalf("Invalid GTID: %v", err)
	}

	// 1. Benchmark Local Search
	fmt.Println("\n[1] Testing Local Search (Disk I/O)...")
	localConfig := &models.Config{
		BinlogDir:    dir,
		TargetGTID:   gtid,
		FilePattern:  "mysql-bin.*",
		StartFile:    startFile,
		Parallel:     4,
		Verbose:      false,
		OutputFormat: models.FormatConsole,
	}

	startLocal := time.Now()
	localSearcher := NewSearcher(localConfig)
	
	// Need to find files first to simulate main.go logic
	files, err := localSearcher.GetBinlogFiles(dir, "mysql-bin.*")
	if err != nil {
		t.Fatalf("Local files error: %v", err)
	}
	
	// Filter files logic (simplified)
	var targetFiles []string
	if startFile != "" {
		found := false
		for _, f := range files {
			if filepath.Base(f) == startFile {
				found = true
			}
			if found {
				targetFiles = append(targetFiles, f)
			}
		}
		if !found {
			t.Fatalf("Start file %s not found in dir", startFile)
		}
	} else {
		targetFiles = files
	}
	
	localRes, err := localSearcher.SearchParallel(targetFiles, &targetGTID)
	durationLocal := time.Since(startLocal)

	if err != nil {
		fmt.Printf("❌ Local Search Failed: %v\n", err)
	} else if localRes != nil {
		fmt.Printf("✅ Local Search Found: %s in %v\n", localRes.GTID, durationLocal)
	} else {
		fmt.Printf("⚠️  Local Search: Not Found\n")
	}

	// 2. Benchmark Remote Search
	fmt.Println("\n[2] Testing Remote Search (Network I/O)...")
	remoteConfig := &models.Config{
		Host:         host,
		Port:         port,
		User:         user,
		Password:     pass,
		TargetGTID:   gtid,
		StartFile:    startFile,
		Verbose:      false,
	}

	startRemote := time.Now()
	remoteSearcher := NewRemoteSearcher(remoteConfig)
	remoteRes, err := remoteSearcher.Search(&targetGTID)
	durationRemote := time.Since(startRemote)

	if err != nil {
		fmt.Printf("❌ Remote Search Failed: %v\n", err)
	} else if remoteRes != nil {
		fmt.Printf("✅ Remote Search Found: %s in %v\n", remoteRes.GTID, durationRemote)
	} else {
		fmt.Printf("⚠️  Remote Search: Not Found\n")
	}

	// 3. Comparison
	fmt.Println("\n📊 Benchmark Results")
	fmt.Println("==================================================")
	fmt.Printf("Local Search : %v\n", durationLocal)
	fmt.Printf("Remote Search: %v\n", durationRemote)
	fmt.Println("==================================================")

	if durationLocal < durationRemote {
		diff := durationRemote - durationLocal
		fmt.Printf("🏆 Local Search is faster by %v\n", diff)
	} else {
		diff := durationLocal - durationRemote
		fmt.Printf("🏆 Remote Search is faster by %v\n", diff)
	}
}
