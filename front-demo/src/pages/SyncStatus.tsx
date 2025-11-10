import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import PageTransition from '../components/PageTransition';
import { RefreshCw } from 'lucide-react';

const clickhouse = createClient({
  url: 'http://localhost:8123',
  username: "anonymous",
});

interface ChainSyncData {
  chain_id: number;
  name: string;
  last_updated: string;
  last_block_on_chain: number;
  watermark_block: number | null;
}

function SyncStatus() {
  const { data: chains, isLoading, error, refetch, isFetching } = useQuery<ChainSyncData[]>({
    queryKey: ['syncStatus'],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT 
            cs.chain_id,
            cs.name,
            cs.last_updated,
            cs.last_block_on_chain,
            sw.block_number as watermark_block
          FROM chain_status cs
          LEFT JOIN sync_watermark sw ON cs.chain_id = sw.chain_id
          ORDER BY cs.chain_id
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<ChainSyncData>();
      return data as ChainSyncData[];
    },
    refetchInterval: 60000,
  });

  const getBlocksBehindHealth = (blocksBehind: number | null) => {
    if (blocksBehind === null) return 'gray';
    if (blocksBehind < 10) return 'green';
    if (blocksBehind < 1000) return 'yellow';
    return 'red';
  };

  const getLastUpdatedHealth = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);

    if (diffSec < 60) return 'green';  // < 1 minute
    if (diffSec < 3600) return 'yellow';  // < 1 hour
    return 'red';  // > 1 hour
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);

    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    return date.toLocaleString();
  };

  const formatNumber = (num: number) => {
    if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
    if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`;
    return num.toLocaleString();
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
                  const blocksBehind = chain.watermark_block !== null
                    ? chain.last_block_on_chain - chain.watermark_block
                    : null;
                  const blocksHealth = getBlocksBehindHealth(blocksBehind);
                  const updatedHealth = getLastUpdatedHealth(chain.last_updated);
                  const syncPercentage = chain.watermark_block !== null
                    ? (chain.watermark_block / chain.last_block_on_chain) * 100
                    : 0;

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
                            {blocksBehind !== null ? formatNumber(blocksBehind) : '—'}
                          </span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="text-sm text-gray-600">
                          {syncPercentage > 0 ? `${syncPercentage.toFixed(2)}%` : '—'}
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
      </div>
    </PageTransition>
  );
}

export default SyncStatus;

