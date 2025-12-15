import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo } from 'react';
import { useNavigate } from 'react-router-dom';
import PageTransition from '../components/PageTransition';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { Network, Users, Scale, Activity, ExternalLink, Copy } from 'lucide-react';
import MetricChart from '../components/MetricChart';

interface GlobalStats {
  total_l1_subnets: number;
  active_validators: number;
  total_weight: string;
  recent_transactions: number;
}

interface SubnetCreationTimeline {
  period: string;
  value: number;
}

interface PlatformActivity {
  tx_type: string;
  count: number;
}

interface L1Subnet {
  subnet_id: string;
  chain_id: string;
  conversion_block: number;
  conversion_time: string;
  validator_count: number;
  name?: string;
  logo_url?: string;
}

interface PChainTx {
  tx_id: string;
  tx_type: string;
  block_number: number;
  block_time: string;
}

function PChainOverview() {
  const { url } = useClickhouseUrl();
  const navigate = useNavigate();

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  // Global Statistics
  const { data: stats, isLoading: loadingStats } = useQuery<GlobalStats>({
    queryKey: ['pchain-stats', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            (SELECT count(DISTINCT subnet_id) FROM l1_subnets) as total_l1_subnets,
            (SELECT count(*) FROM l1_validator_state WHERE active = true) as active_validators,
            (SELECT toString(sum(weight)) FROM l1_validator_state WHERE active = true) as total_weight,
            (SELECT count(*) FROM p_chain_txs WHERE block_time >= now() - INTERVAL 7 DAY) as recent_transactions
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<GlobalStats>();
      return (data as GlobalStats[])[0];
    },
    refetchInterval: 30000,
  });

  // Subnet Creation Timeline
  const { data: timeline, isLoading: loadingTimeline } = useQuery<SubnetCreationTimeline[]>({
    queryKey: ['subnet-timeline', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            formatDateTime(toStartOfMonth(conversion_time), '%Y-%m-%d') as period,
            count() as value
          FROM l1_subnets
          GROUP BY period
          ORDER BY period
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<SubnetCreationTimeline>();
      
      // Calculate cumulative sum
      let cumulative = 0;
      return data.map(item => {
        cumulative += item.value;
        return {
          ...item,
          value: cumulative
        };
      });
    },
    refetchInterval: 60000,
  });

  // Recent Platform Activity
  const { data: platformActivity, isLoading: loadingActivity } = useQuery<PlatformActivity[]>({
    queryKey: ['platform-activity', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            tx_type,
            count() as count
          FROM p_chain_txs
          WHERE block_time >= now() - INTERVAL 30 DAY
          GROUP BY tx_type
          ORDER BY count DESC
          LIMIT 10
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<PlatformActivity>();
      return data as PlatformActivity[];
    },
    refetchInterval: 30000,
  });

  // L1 Subnets Table
  const { data: subnets, isLoading: loadingSubnets } = useQuery<L1Subnet[]>({
    queryKey: ['l1-subnets-v4', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          WITH validator_counts AS (
            SELECT
              subnet_id,
              count(*) as validator_count
            FROM l1_validator_state FINAL
            WHERE active = true
            GROUP BY subnet_id
          )
          SELECT
            s.subnet_id as subnet_id,
            s.chain_id as chain_id,
            s.conversion_block as conversion_block,
            formatDateTime(s.conversion_time, '%Y-%m-%d %H:%i:%s') as conversion_time,
            COALESCE(v.validator_count, 0) as validator_count,
            NULLIF(r.name, '') as name,
            NULLIF(r.logo_url, '') as logo_url
          FROM l1_subnets AS s FINAL
          LEFT JOIN l1_registry AS r FINAL ON s.subnet_id = r.subnet_id
          LEFT JOIN validator_counts AS v ON s.subnet_id = v.subnet_id
          ORDER BY validator_count DESC, s.conversion_time DESC
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<L1Subnet>();
      return data as L1Subnet[];
    },
    refetchInterval: 30000,
  });

  // Recent P-Chain Transactions
  const { data: recentTxs, isLoading: loadingTxs } = useQuery<PChainTx[]>({
    queryKey: ['recent-txs', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            base58Encode(concat(tx_id, substring(SHA256(SHA256(tx_id)), 1, 4))) as tx_id,
            tx_type,
            block_number,
            toUnixTimestamp(block_time) as timestamp
          FROM p_chain_txs
          WHERE block_time >= now() - INTERVAL 1 DAY
          ORDER BY block_time DESC
          LIMIT 10
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<any>();
      return data.map((item: any) => ({
        ...item,
        block_time: new Date(item.timestamp * 1000).toISOString() // Convert to ISO string for formatTimestamp
      })) as PChainTx[];
    },
    refetchInterval: 15000,
  });

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSec = Math.floor(diffMs / 1000);
    const diffMin = Math.floor(diffSec / 60);
    const diffHour = Math.floor(diffMin / 60);
    const diffDays = Math.floor(diffMs / (1000 * 60 * 60 * 24));

    if (diffSec < 60) return `${diffSec}s ago`;
    if (diffMin < 60) return `${diffMin}m ago`;
    if (diffHour < 24) return `${diffHour}h ago`;
    if (diffDays === 1) return 'Yesterday';
    if (diffDays < 7) return `${diffDays}d ago`;
    if (diffDays < 30) return `${Math.floor(diffDays / 7)}w ago`;
    return date.toLocaleDateString();
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const truncateHash = (hash: string, length = 8) => {
    if (!hash) return '';
    return `${hash.slice(0, length)}...${hash.slice(-4)}`;
  };

  const formatWeight = (weight: string) => {
    const num = parseFloat(weight);
    if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
    if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
    if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K`;
    return num.toFixed(0);
  };

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        {/* Header */}
        <div>
          <h1 className="text-3xl font-bold text-gray-900">P-Chain Overview (v2)</h1>
          <p className="text-gray-600 mt-2">
            Platform chain for L1 subnet creation and validator management
          </p>
        </div>

        {/* Global Stats Cards */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
          {/* Total L1 Subnets */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  L1 Subnets
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.total_l1_subnets || 0}
                </p>
              </div>
              <div className="p-3 bg-blue-100 rounded-full">
                <Network size={24} className="text-blue-600" />
              </div>
            </div>
          </div>

          {/* Active Validators */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-green-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Active Validators
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.active_validators || 0}
                </p>
              </div>
              <div className="p-3 bg-green-100 rounded-full">
                <Users size={24} className="text-green-600" />
              </div>
            </div>
          </div>

          {/* Total Weight */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-purple-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Total Weight
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : formatWeight(stats?.total_weight || '0')}
                </p>
              </div>
              <div className="p-3 bg-purple-100 rounded-full">
                <Scale size={24} className="text-purple-600" />
              </div>
            </div>
          </div>

          {/* Recent Transactions */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-orange-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Transactions (7d)
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.recent_transactions || 0}
                </p>
              </div>
              <div className="p-3 bg-orange-100 rounded-full">
                <Activity size={24} className="text-orange-600" />
              </div>
            </div>
          </div>
        </div>

        {/* Two Column Layout */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Subnet Creation Timeline Chart */}
          <div className="bg-white rounded-lg shadow overflow-hidden">
            {loadingTimeline ? (
              <div className="h-[350px] flex items-center justify-center">
                <p className="text-gray-500">Loading timeline...</p>
              </div>
            ) : (
              <MetricChart
                metricName="Cumulative L1 Subnets"
                data={timeline?.map(t => ({
                  chain_id: 0,
                  metric_name: 'total_l1_subnets',
                  granularity: 'month',
                  period: t.period,
                  value: t.value,
                  computed_at: new Date().toISOString()
                })) || []}
                granularity="month"
              />
            )}
          </div>

          {/* Platform Activity */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-bold text-gray-900 mb-4">Platform Activity (30d)</h2>
            <p className="text-sm text-gray-600 mb-4">P-chain management transactions by type</p>
            {loadingActivity ? (
              <div className="h-64 flex items-center justify-center">
                <p className="text-gray-500">Loading activity...</p>
              </div>
            ) : platformActivity && platformActivity.length > 0 ? (
              <div className="space-y-1">
                {platformActivity.map((item, idx) => (
                  <div key={idx} className="flex items-center justify-between py-2 border-b border-gray-100 last:border-0">
                    <span className="text-sm font-medium text-gray-700">{item.tx_type}</span>
                    <span className="text-sm font-bold text-gray-900">{item.count.toLocaleString()}</span>
                  </div>
                ))}
              </div>
            ) : (
              <div className="h-64 flex items-center justify-center">
                <p className="text-gray-500">No platform activity data available</p>
              </div>
            )}
          </div>
        </div>

        {/* L1 Subnets Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-xl font-bold text-gray-900">L1 Subnets</h2>
          </div>

          {loadingSubnets ? (
            <div className="p-8 text-center">
              <p className="text-gray-500">Loading subnets...</p>
            </div>
          ) : subnets && subnets.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Subnet ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Chain ID
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Conversion Block
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Created
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Validators
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Actions
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {subnets.map((subnet, idx) => (
                    <tr key={subnet.subnet_id} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          {subnet.logo_url && (
                            <img
                              src={subnet.logo_url}
                              alt={subnet.name || 'Subnet logo'}
                              className="w-6 h-6 rounded-full object-cover"
                              onError={(e) => { e.currentTarget.style.display = 'none'; }}
                            />
                          )}
                          <span className={`text-sm ${subnet.name ? 'font-medium text-gray-900' : 'font-mono text-gray-500'}`}>
                            {subnet.name || truncateHash(subnet.subnet_id, 10)}
                          </span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          <code className="text-xs font-mono text-gray-900">
                            {truncateHash(subnet.subnet_id, 10)}
                          </code>
                          <button
                            onClick={() => copyToClipboard(subnet.subnet_id)}
                            className="text-gray-400 hover:text-gray-600 transition-colors"
                            title="Copy full subnet ID"
                          >
                            <Copy size={14} />
                          </button>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <code className="text-xs font-mono text-gray-600">
                          {truncateHash(subnet.chain_id, 10)}
                        </code>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm text-gray-900">
                          {subnet.conversion_block.toLocaleString()}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className="text-sm text-gray-600">
                          {formatTimestamp(subnet.conversion_time)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">
                          {subnet.validator_count}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <button
                          onClick={() => navigate(`/p-chain/subnet/${subnet.subnet_id}`)}
                          className="text-blue-600 hover:text-blue-800 text-sm font-medium flex items-center gap-1 transition-colors"
                        >
                          View Details
                          <ExternalLink size={14} />
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center">
              <p className="text-gray-500">No L1 subnets found</p>
            </div>
          )}
        </div>

        {/* Recent P-Chain Transactions */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-xl font-bold text-gray-900">Recent Transactions</h2>
          </div>

          {loadingTxs ? (
            <div className="p-8 text-center">
              <p className="text-gray-500">Loading transactions...</p>
            </div>
          ) : recentTxs && recentTxs.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Tx ID
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Type
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Block
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Time
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {recentTxs.map((tx, idx) => (
                    <tr key={tx.tx_id} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          <code className="text-xs font-mono text-gray-900">
                            {truncateHash(tx.tx_id, 12)}
                          </code>
                          <button
                            onClick={() => copyToClipboard(tx.tx_id)}
                            className="text-gray-400 hover:text-gray-600 transition-colors"
                            title="Copy full Tx ID"
                          >
                            <Copy size={14} />
                          </button>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          {tx.tx_type}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm text-gray-900">
                          {tx.block_number.toLocaleString()}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className="text-sm text-gray-600">
                          {formatTimestamp(tx.block_time)}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center">
              <p className="text-gray-500">No recent transactions found</p>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default PChainOverview;
