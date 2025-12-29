# Binlog Info - MySQL GTID Position Finder

🔍 **Fast CLI tool** để tìm GTID trong MySQL binlog files và trả về **resume position** tương thích với Kafka Connect/Debezium.

## ✨ Features

- ⚡ **Parallel Processing** - Scan nhiều binlog files đồng thời
- 🎯 **Resume Position** - Trả về position tương thích Kafka Connect
- 📊 **Multiple Output Formats** - Console, CSV, JSON
- 🔄 **Transaction Boundary Tracking** - Phân biệt Start/Commit/Resume positions
- 🧪 **Well Tested** - Comprehensive unit tests

## 📍 Position Types Explained

Tool trả về 3 loại position quan trọng:

| Position | Mô tả | Sử dụng |
|----------|-------|---------|
| **Start Position** | Vị trí bắt đầu GTID event | Debug, xem transaction start |
| **Commit Position** | END_LOG_POS của XID event (kết thúc transaction) | Xác nhận transaction hoàn tất |
| **Resume Position** | END_LOG_POS của GTID event tiếp theo | **Kafka Connect**, CDC tools |

### Why Resume Position ≠ Commit Position?

```
Transaction N:
  GTID Event (start=1000, end=1065)
  Query/Row Events...
  XID Event (end=2000)          ← Commit Position

Transaction N+1:
  GTID Event (start=2000, end=2065)  ← Resume Position = 2065
```

**Kafka Connect** lưu `pos: 2065` (END_LOG_POS của GTID event tiếp theo) để khi resume, nó sẽ bắt đầu đọc từ transaction tiếp theo.

## 📦 Installation

```bash
# Clone và build
git clone https://github.com/quyetmv/mysql-gtid-position.git
cd binlog-info
make build

# Binary: bin/binlog-info
```

## 🚀 Usage

### Basic Usage

```bash
# Tìm GTID và lấy resume position
./binlog-info \
  -dir /data/log \
  -gtid "7396024d-8ec5-11f0-b6ea-fa163e91516e:1-5795043"
```

**Output:**
```
✅ Found GTID
📄 Binlog File: /data/log/mysql-bin.000004
🆔 GTID: 7396024d-8ec5-11f0-b6ea-fa163e91516e:5795043

📍 Start Position (GTID):     1025441563
📍 Commit Position (Xid):     1025445254
📍 Resume Position:           1025445319   ✅
🔄 Next GTID:                 7396024d-8ec5-11f0-b6ea-fa163e91516e:5795044

🕐 Timestamp: 2025-12-29T15:09:47+07:00
💾 Database: mydb
```

### Start from Specific File (Faster)

```bash
# Skip older files, start từ file cụ thể
./binlog-info \
  -dir /data/log \
  -gtid "UUID:1-5795043" \
  -start-file "mysql-bin.000100"
```

### JSON Output (for automation)

```bash
./binlog-info \
  -dir /data/log \
  -gtid "UUID:1-5795043" \
  -format json
```

```json
{
  "binlog_file": "/data/log/mysql-bin.000004",
  "start_position": 1025441563,
  "commit_position": 1025445254,
  "resume_position": 1025445319,
  "gtid": "7396024d-8ec5-11f0-b6ea-fa163e91516e:5795043",
  "next_gtid": "7396024d-8ec5-11f0-b6ea-fa163e91516e:5795044",
  "database": "mydb",
  "timestamp": 1735459787
}
```

### Filter by Database

```bash
./binlog-info \
  -dir /data/log \
  -gtid "UUID:1-100" \
  -database "mydb"
```

### Filter by Time Range

```bash
./binlog-info \
  -dir /data/log \
  -gtid "UUID:1-100" \
  -start-time "2025-01-01 00:00:00" \
  -end-time "2025-01-02 00:00:00"
```

### Parallel Processing

```bash
# 8 workers để scan nhiều files cùng lúc
./binlog-info \
  -dir /data/log \
  -gtid "UUID:1-100" \
  -parallel 8
```

> **Note**: `-parallel` chỉ hiệu quả khi có nhiều binlog files. Với 2-3 files, thời gian chủ yếu là disk I/O.

## 🎯 Use Cases

### 1. Kafka Connect Resume Position

Khi Kafka Connect bị crash, bạn cần tìm position để resume:

```bash
# Lấy GTID từ Kafka Connect offset
GTID="7396024d-8ec5-11f0-b6ea-fa163e91516e:1-5795043"

# Tìm resume position
./binlog-info -dir /data/log -gtid "$GTID" -format json

# Configure Kafka Connect với:
# - file: mysql-bin.000004
# - pos: 1025445319 (resume_position)
```

### 2. Debezium Snapshot Recovery

```bash
./binlog-info \
  -dir /data/log \
  -gtid "$DEBEZIUM_GTID" \
  -format json > position.json
```

### 3. MySQL Replication Setup

```bash
# Tìm position để setup slave
./binlog-info \
  -dir /data/log \
  -gtid "$MASTER_GTID" \
  -verbose
```

### 4. Point-in-Time Recovery

```bash
./binlog-info \
  -dir /data/log \
  -gtid "$PITR_GTID" \
  -end-time "2025-01-15 10:30:00"
```

## 🔧 Command-line Flags

| Flag | Type | Default | Description |
|------|------|---------|-------------|
| `-dir` | string | (required) | Binlog directory path |
| `-gtid` | string | (required) | Target GTID set to find |
| `-pattern` | string | mysql-bin.* | Binlog file pattern |
| `-start-file` | string | - | Start from specific binlog file |
| `-parallel` | int | 4 | Number of parallel workers |
| `-format` | string | console | Output: console, csv, json |
| `-output` | string | stdout | Output file path |
| `-database` | string | - | Filter by database name |
| `-start-time` | string | - | Filter events after time |
| `-end-time` | string | - | Filter events before time |
| `-verbose` | bool | false | Show detailed progress |
| `-find-active-master` | bool | false | Find UUID with highest GNO |
| `-uuid` | string | - | Filter by specific UUID |

## 📊 Output Formats

### Console (default)
Human-readable output với emojis và formatting.

### CSV
```csv
binlog_file,start_position,commit_position,resume_position,gtid,next_gtid,timestamp,database
/data/log/mysql-bin.000004,1025441563,1025445254,1025445319,UUID:5795043,UUID:5795044,1735459787,mydb
```

### JSON
```json
{
  "binlog_file": "...",
  "start_position": 1025441563,
  "commit_position": 1025445254,
  "resume_position": 1025445319,
  "gtid": "UUID:5795043",
  "next_gtid": "UUID:5795044",
  "timestamp": 1735459787,
  "database": "mydb"
}
```

## 🏗️ How It Works

1. **Parse GTID Set**: Phân tích target GTID range (e.g., `UUID:1-5795043`)
2. **Scan Binlog Files**: Scan tuần tự hoặc song song các binlog files
3. **Track Transactions**: 
   - Detect GTID event (transaction start)
   - Track XID event (transaction commit)
   - Capture next GTID event (resume position)
4. **Find Highest GNO**: Trong range, trả về transaction có GNO cao nhất
5. **Return Positions**: Trả về start/commit/resume positions

## �️ Development

```bash
# Run tests
make test

# Run with coverage
make test-coverage

# Build
make build

# Clean
make clean
```

## Support

If you find this project helpful or valuable, please consider supporting its development by buying me a coffee!

<a href="https://www.buymeacoffee.com/quyetmv" target="_blank"><img src="https://cdn.buymeacoffee.com/buttons/v2/default-yellow.png" alt="Buy Me A Coffee" width="180" height="45" ></a>

## Contact

For any questions or inquiries, please contact me:

- Email: [<img src="https://upload.wikimedia.org/wikipedia/commons/7/7e/Gmail_icon_%282020%29.svg" alt="Email" height="15" width="15"> quyetmv@gmail.com](mailto:quyetmv@gmail.com)
- Telegram: [<img src="https://upload.wikimedia.org/wikipedia/commons/8/82/Telegram_logo.svg" alt="Telegram" height="15" width="15"> quyetmv](https://t.me/quyetmv)


## 📝 License

MIT License

---

**Made with ❤️ for MySQL DBAs and DevOps engineers**
