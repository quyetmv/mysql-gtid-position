#!/bin/bash
# Example: MySQL Migration with GTID using binlog-info

set -e

# Configuration
SOURCE_HOST="mysql-source"
TARGET_HOST="mysql-target"
BINLOG_DIR="/var/lib/mysql"
GTID="$1"

if [ -z "$GTID" ]; then
    echo "Usage: $0 <GTID>"
    echo "Example: $0 3E11FA47-71CA-11E1-9E33-C80AA9429562:23"
    exit 1
fi

echo "🔍 Finding GTID position..."
../bin/binlog-info \
    -dir "$BINLOG_DIR" \
    -gtid "$GTID" \
    -format csv \
    -output /tmp/gtid_position.csv

if [ ! -f /tmp/gtid_position.csv ]; then
    echo "❌ Failed to find GTID position"
    exit 1
fi

# Parse CSV output
BINLOG_FILE=$(awk -F',' 'NR==2 {print $1}' /tmp/gtid_position.csv)
BINLOG_POS=$(awk -F',' 'NR==2 {print $2}' /tmp/gtid_position.csv)

echo "✅ Found position:"
echo "   Binlog File: $BINLOG_FILE"
echo "   Position: $BINLOG_POS"

# Setup replication (commented out for safety)
# echo "🔧 Setting up replication on target..."
# mysql -h "$TARGET_HOST" -e "
#   CHANGE MASTER TO
#     MASTER_HOST='$SOURCE_HOST',
#     MASTER_LOG_FILE='$(basename $BINLOG_FILE)',
#     MASTER_LOG_POS=$BINLOG_POS;
#   START SLAVE;
# "

echo "✅ Migration script completed"
echo "📝 Next steps:"
echo "   1. Review the position above"
echo "   2. Uncomment replication setup in this script"
echo "   3. Run again to setup replication"
