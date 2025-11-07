# 1. Drop ClickHouse's internal caches
clickhouse-client --query "SYSTEM DROP MARK CACHE"
clickhouse-client --query "SYSTEM DROP UNCOMPRESSED CACHE"
clickhouse-client --query "SYSTEM DROP COMPILED EXPRESSION CACHE"
clickhouse-client --query "SYSTEM DROP QUERY CACHE"

# 2. Drop OS page cache (requires root)
sync
echo 3 > /proc/sys/vm/drop_caches

# 3. Now run your benchmark
# Generate queries with random block numbers
MIN=$(clickhouse-client --query "select min(block_number) from raw_txs")
MAX=$(clickhouse-client --query "select max(block_number) from raw_txs")
echo "We are querying $((MAX - MIN + 1)) blocks between $MIN and $MAX"

for i in {1..100000}; do
  BLOCK=$((MIN + RANDOM % (MAX - MIN + 1)))
  echo "select * from raw_txs where chain_id=43114 and block_number=$BLOCK and transaction_index=0"
done | clickhouse-benchmark -t10 -c 64

