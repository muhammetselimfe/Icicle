import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import PageTransition from '../components/PageTransition';
import { RefreshCw } from 'lucide-react';
import { useMemo } from 'react';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { useSyncStatus } from '../hooks/useSyncStatus';

interface TableSize {
  table: string;
  size_bytes: number;
  rows: number;
}

interface ChainTxCount {
  chain_id: number;
  tx_count: number;
}

function SyncStatus() {
  const { url } = useClickhouseUrl();

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  // Use shared sync status hook
  const { chains, isLoading, error, refetch, isFetching } = useSyncStatus();

  const { data: chainTxCounts } = useQuery<ChainTxCount[]>({
    queryKey: ['chainTxCounts', url],
    queryFn: async () => {
      // Use system.parts to get row counts without scanning data
      const result = await clickhouse.query({
        query: `
          SELECT
            0 as chain_id,
            sum(rows) as tx_count
          FROM system.parts
          WHERE active
            AND database = currentDatabase()
            AND table = 'p_chain_txs'
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<ChainTxCount>();
      return data as ChainTxCount[];
    },
    refetchInterval: 60000,
  });

  const { data: tableSizes, isLoading: isLoadingTables, error: tableSizesError } = useQuery<TableSize[]>({
    queryKey: ['tableSizes', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT 
            table,
            sum(bytes) as size_bytes,
            sum(rows) as rows
          FROM system.parts
          WHERE active
              AND database = currentDatabase()
          GROUP BY table
          ORDER BY sum(bytes) DESC
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<TableSize>();
      return data as TableSize[];
    },
    refetchInterval: 60000,
  });

  const getBlocksBehindHealth = (blocksBehind: number | null) => {
    if (blocksBehind === null) return 'gray';
    if (blocksBehind < 10) return 'green';
    if (blocksBehind < 1000) return 'yellow';
    return 'red';
  };

  const getLastUpdatedHealth = (unixTimestamp: number) => {
    const nowSec = Math.floor(Date.now() / 1000);
    const diffSec = nowSec - unixTimestamp;

    if (diffSec < 60) return 'green';  // < 1 minute
    if (diffSec < 3600) return 'yellow';  // < 1 hour
    return 'red';  // > 1 hour
  };

  const formatTimestamp = (unixTimestamp: number) => {
    const nowSec = Math.floor(Date.now() / 1000);
    const diffSec = nowSec - unixTimestamp;
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);

    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    return new Date(unixTimestamp * 1000).toLocaleString();
  };

  const getHealthDot = (health: string) => {
    const colors = {
      green: 'bg-green-500',
      yellow: 'bg-yellow-500',
      red: 'bg-red-500',
      gray: 'bg-gray-400',
    };
    return colors[health as keyof typeof colors] || colors.gray;
  };

  const formatBytes = (bytes: number): string => {
    const gb = bytes / (1024 * 1024 * 1024);
    return `${gb.toFixed(3)} GB`;
  };

  const formatBytesPerRow = (bytes: number): string => {
    if (bytes < 1024) return `${bytes.toFixed(0)} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(2)} KB`;
    if (bytes < 1024 * 1024 * 1024) return `${(bytes / (1024 * 1024)).toFixed(2)} MB`;
    return `${(bytes / (1024 * 1024 * 1024)).toFixed(2)} GB`;
  };

  const totalRows = tableSizes?.reduce((sum, table) => sum + table.rows, 0) || 0;
  const totalBytes = tableSizes?.reduce((sum, table) => sum + table.size_bytes, 0) || 0;

  const rawTxsTable = tableSizes?.find(table => table.table === 'raw_txs');
  const rawTxsCount = rawTxsTable?.rows || 0;

  // Calculate GB per 1B transactions
  const gbPer1BTxs = rawTxsCount > 0
    ? (totalBytes / rawTxsCount) * 1_000_000_000 / (1024 * 1024 * 1024)
    : 0;

  // Create a map of chain_id to tx_count for quick lookup
  const txCountMap = useMemo(() => {
    if (!chainTxCounts) return new Map<number, number>();
    return new Map(chainTxCounts.map(c => [c.chain_id, c.tx_count]));
  }, [chainTxCounts]);

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        {/* Header */}
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-3xl font-bold text-gray-900">Sync Status</h1>
            {chains && (
              <p className="text-sm text-gray-600 mt-1">
                Monitoring {chains.length} chain{chains.length !== 1 ? 's' : ''}
              </p>
            )}
          </div>
          <button
            onClick={() => refetch()}
            disabled={isFetching}
            className="p-2 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded-lg transition-colors disabled:opacity-50 cursor-pointer"
            title="Refresh"
          >
            <RefreshCw size={20} className={isFetching ? 'animate-spin' : ''} />
          </button>
        </div>

        {/* Storage Efficiency Metric */}
        {tableSizes && rawTxsCount > 0 && (
          <div className="bg-gradient-to-r from-blue-50 to-indigo-50 border border-blue-200 rounded-lg p-6">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wider mb-1">
                  Storage Efficiency
                </h3>
                <p className="text-3xl font-bold text-gray-900">
                  {gbPer1BTxs.toFixed(2)} GB
                </p>
                <p className="text-sm text-gray-600 mt-1">
                  per 1 billion transactions
                </p>
              </div>
              <div className="text-right">
                <p className="text-sm text-gray-600">
                  Total DB: {formatBytes(totalBytes)}
                </p>
                <p className="text-sm text-gray-600">
                  Raw TXs: {rawTxsCount.toLocaleString()}
                </p>
              </div>
            </div>
          </div>
        )}

        {/* Loading State */}
        {isLoading && (
          <div className="bg-white rounded-lg shadow p-8 text-center">
            <p className="text-gray-600">Loading sync status...</p>
          </div>
        )}

        {/* Error State */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-6">
            <h3 className="text-sm font-semibold text-red-900 mb-1">Error Loading Sync Status</h3>
            <p className="text-sm text-red-700">{error.message}</p>
          </div>
        )}

        {/* Status Table */}
        {chains && chains.length > 0 && (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <table className="w-full">
              <thead className="bg-gray-50 border-b border-gray-200">
                <tr>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Chain
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Chain ID
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Transactions
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Synced Block
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Latest Block
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Blocks Behind
                  </th>
                  <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Sync %
                  </th>
                  <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                    Last Updated
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-200">
                {chains.map((chain, idx) => {
                  const blocksHealth = getBlocksBehindHealth(chain.blocksBehind);
                  const updatedHealth = getLastUpdatedHealth(chain.last_updated);
                  const txCount = txCountMap.get(chain.chain_id) || 0;

                  return (
                    <tr
                      key={chain.chain_id}
                      className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}
                    >
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm font-medium text-gray-900">{chain.name}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-sm text-gray-600">{chain.chain_id}</div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="text-sm font-medium text-gray-900">
                          {txCount.toLocaleString()}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="text-sm font-medium text-gray-900">
                          {chain.watermark_block !== null ? chain.watermark_block.toLocaleString() : '—'}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="text-sm font-medium text-gray-900">
                          {chain.last_block_on_chain.toLocaleString()}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="flex items-center justify-end gap-2">
                          <div className={`w-2 h-2 rounded-full ${getHealthDot(blocksHealth)}`} />
                          <span className="text-sm font-semibold text-gray-900">
                            {chain.blocksBehind !== null ? chain.blocksBehind.toLocaleString() : '—'}
                          </span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="text-sm text-gray-600">
                          {chain.syncPercentage > 0 ? `${chain.syncPercentage.toFixed(2)}%` : '—'}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          <div className={`w-2 h-2 rounded-full ${getHealthDot(updatedHealth)}`} />
                          <span className="text-sm text-gray-600">
                            {formatTimestamp(chain.last_updated)}
                          </span>
                        </div>
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Empty State */}
        {!isLoading && !error && chains && chains.length === 0 && (
          <div className="bg-white rounded-lg shadow p-8 text-center">
            <p className="text-gray-600">No chains found in the database.</p>
          </div>
        )}

        {/* Table Sizes */}
        <div className="mt-8">
          <h2 className="text-2xl font-bold text-gray-900 mb-4">Database Table Sizes</h2>
          {isLoadingTables && (
            <div className="bg-white rounded-lg shadow p-8 text-center">
              <p className="text-gray-600">Loading table sizes...</p>
            </div>
          )}

          {tableSizesError && (
            <div className="bg-red-50 border border-red-200 rounded-lg p-6">
              <h3 className="text-sm font-semibold text-red-900 mb-1">Error Loading Table Sizes</h3>
              <p className="text-sm text-red-700">{tableSizesError.message}</p>
            </div>
          )}

          {tableSizes && tableSizes.length > 0 && (
            <div className="bg-white rounded-lg shadow overflow-hidden">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Table Name
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Rows
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Size
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Bytes/Row
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {tableSizes.map((table, idx) => {
                    const bytesPerRow = table.rows > 0 ? table.size_bytes / table.rows : 0;
                    return (
                      <tr
                        key={table.table}
                        className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}
                      >
                        <td className="px-6 py-4 whitespace-nowrap">
                          <div className="text-sm font-medium text-gray-900">{table.table}</div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm text-gray-900">
                            {table.rows.toLocaleString()}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm font-medium text-gray-900">
                            {formatBytes(table.size_bytes)}
                          </div>
                        </td>
                        <td className="px-6 py-4 whitespace-nowrap text-right">
                          <div className="text-sm text-gray-600">
                            {formatBytesPerRow(bytesPerRow)}
                          </div>
                        </td>
                      </tr>
                    );
                  })}
                  <tr className="bg-gray-100 border-t-2 border-gray-300">
                    <td className="px-6 py-4 whitespace-nowrap">
                      <div className="text-sm font-bold text-gray-900">TOTAL</div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right">
                      <div className="text-sm font-bold text-gray-900">
                        {totalRows.toLocaleString()}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right">
                      <div className="text-sm font-bold text-gray-900">
                        {formatBytes(totalBytes)}
                      </div>
                    </td>
                    <td className="px-6 py-4 whitespace-nowrap text-right">
                      <div className="text-sm font-bold text-gray-600">
                        {formatBytesPerRow(totalRows > 0 ? totalBytes / totalRows : 0)}
                      </div>
                    </td>
                  </tr>
                </tbody>
              </table>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default SyncStatus;

