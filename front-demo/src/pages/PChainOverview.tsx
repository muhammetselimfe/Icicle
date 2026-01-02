import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo, useState } from 'react';
import { useNavigate, Link } from 'react-router-dom';
import PageTransition from '../components/PageTransition';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { Network, Users, Copy, Search, Coins } from 'lucide-react';
import MetricChart from '../components/MetricChart';

interface GlobalStats {
  active_chains: number;
  active_legacy_subnets: number;
  active_l1_subnets: number;
  active_validators: number;
  recent_transactions: number;
  total_l1_fees_paid: number;
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
  subnet_type: string;
  chain_id: string;
  conversion_block: number;
  conversion_time: string;
  created_block: number;
  created_time: string;
  validator_count: number;
  name?: string;
  logo_url?: string;
  total_fees_paid?: number;
}

interface PChainBlock {
  block_number: number;
  block_time: string;
  tx_count: number;
  block_hash?: string;
}

interface RecentTransaction {
  tx_id: string;
  tx_type: string;
  formatted_time: string;
  block_number: number;
}

function PChainOverview() {
  const { url } = useClickhouseUrl();
  const navigate = useNavigate();
  const [searchTerm, setSearchTerm] = useState('');
  const [txBlockSearch, setTxBlockSearch] = useState('');
  const [showAllChains, setShowAllChains] = useState(false);
  const [typeFilter, setTypeFilter] = useState<'all' | 'l1' | 'legacy'>('all');

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
    max_open_connections: 10,
    request_timeout: 30000,
  }), [url]);

  // Global Statistics
  const { data: stats, isLoading: loadingStats } = useQuery<GlobalStats>({
    queryKey: ['pchain-stats', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            -- Active L1 subnets: L1 subnets with at least 1 active validator
            (SELECT count(DISTINCT v.subnet_id) FROM l1_validator_state v FINAL JOIN subnets s FINAL ON v.subnet_id = s.subnet_id WHERE v.active = true AND s.subnet_type = 'l1') as active_l1_subnets,
            -- Active legacy subnets: regular subnets with at least 1 active validator
            (SELECT count(DISTINCT v.subnet_id) FROM l1_validator_state v FINAL JOIN subnets s FINAL ON v.subnet_id = s.subnet_id WHERE v.active = true AND s.subnet_type = 'regular') as active_legacy_subnets,
            -- Total active chains: sum of active L1s, active legacy subnets, and Primary Network
            (SELECT count(DISTINCT v.subnet_id) FROM l1_validator_state v FINAL JOIN subnets s FINAL ON v.subnet_id = s.subnet_id WHERE v.active = true AND s.subnet_type IN ('l1', 'regular', 'primary')) as active_chains,
            -- Count unique validators: Primary Network + L1 subnets only (legacy subnet validators are already Primary Network validators)
            (SELECT count(DISTINCT node_id) FROM l1_validator_state v FINAL JOIN subnets s FINAL ON v.subnet_id = s.subnet_id WHERE v.active = true AND s.subnet_type IN ('primary', 'l1')) as active_validators,
            (SELECT count(*) FROM p_chain_txs WHERE p_chain_id = 0 AND block_time >= now() - INTERVAL 7 DAY) as recent_transactions,
            -- Total L1 validation fees paid (in nAVAX)
            (SELECT sum(total_fees_paid) FROM l1_fee_stats FINAL WHERE p_chain_id = 0) as total_l1_fees_paid
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
            formatDateTime(toStartOfMonth(converted_time), '%Y-%m-%d') as period,
            count() as value
          FROM subnets FINAL
          WHERE subnet_type = 'l1' AND converted_time > toDateTime('1970-01-01 00:00:01')
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
          WHERE p_chain_id = 0
            AND block_time >= now() - INTERVAL 30 DAY
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

  // All Subnets Table
  const { data: subnets, isLoading: loadingSubnets } = useQuery<L1Subnet[]>({
    queryKey: ['all-subnets-v2', url],
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
            s.subnet_type as subnet_type,
            s.chain_id as chain_id,
            s.created_block as created_block,
            formatDateTime(s.created_time, '%Y-%m-%d %H:%i:%s') as created_time,
            s.converted_block as conversion_block,
            formatDateTime(s.converted_time, '%Y-%m-%d %H:%i:%s') as conversion_time,
            COALESCE(v.validator_count, 0) as validator_count,
            NULLIF(r.name, '') as name,
            NULLIF(r.logo_url, '') as logo_url,
            COALESCE(f.total_fees_paid, 0) as total_fees_paid
          FROM subnets AS s FINAL
          LEFT JOIN l1_registry AS r FINAL ON s.subnet_id = r.subnet_id
          LEFT JOIN validator_counts AS v ON s.subnet_id = v.subnet_id
          LEFT JOIN l1_fee_stats AS f FINAL ON s.subnet_id = f.subnet_id
          ORDER BY
            CASE s.subnet_type
              WHEN 'primary' THEN 0
              WHEN 'l1' THEN 1
              WHEN 'elastic' THEN 2
              WHEN 'regular' THEN 3
              ELSE 4
            END,
            validator_count DESC,
            s.created_time DESC
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<L1Subnet>();
      return data as L1Subnet[];
    },
    refetchInterval: 30000,
  });

  // Recent P-Chain Blocks
  const { data: recentBlocks, isLoading: loadingBlocks } = useQuery<PChainBlock[]>({
    queryKey: ['recent-blocks', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            block_number,
            toUnixTimestamp(any(block_time)) as timestamp,
            count(*) as tx_count,
            substring(any(tx_id), 1, 12) as block_hash
          FROM p_chain_txs
          WHERE p_chain_id = 0
            AND block_time >= now() - INTERVAL 1 DAY
          GROUP BY block_number
          ORDER BY block_number DESC
          LIMIT 8
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<any>();
      return data.map((item: any) => ({
        ...item,
        block_time: new Date(item.timestamp * 1000).toISOString()
      })) as PChainBlock[];
    },
    refetchInterval: 15000,
  });

  // Recent P-Chain Transactions
  const { data: recentTransactions, isLoading: loadingTransactions, error: txError } = useQuery<RecentTransaction[]>({
    queryKey: ['recent-transactions', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            tx_id,
            tx_type,
            formatDateTime(block_time, '%Y-%m-%dT%H:%i:%sZ') as formatted_time,
            block_number
          FROM p_chain_txs
          WHERE p_chain_id = 0
            AND block_time >= now() - INTERVAL 1 DAY
          ORDER BY block_time DESC
          LIMIT 8
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<RecentTransaction>();
      console.log('Recent transactions:', data);
      return data as RecentTransaction[];
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

  const formatSubnetType = (type: string) => {
    switch (type) {
      case 'l1': return 'L1';
      case 'regular': return 'Legacy Subnet';
      case 'elastic': return 'Elastic';
      case 'primary': return 'Primary';
      default: return type;
    }
  };

  // Format nAVAX to AVAX with appropriate decimal places
  const formatAVAX = (nanoAVAX: number, decimals = 2) => {
    const avax = nanoAVAX / 1e9; // 1 AVAX = 1e9 nAVAX
    if (avax >= 1000000) {
      return `${(avax / 1000000).toFixed(decimals)}M`;
    } else if (avax >= 1000) {
      return `${(avax / 1000).toFixed(decimals)}K`;
    } else if (avax >= 1) {
      return avax.toFixed(decimals);
    } else {
      return avax.toFixed(4);
    }
  };

  const handleSearch = (e: React.FormEvent) => {
    e.preventDefault();
    const trimmedSearch = txBlockSearch.trim();

    if (!trimmedSearch) return;

    // Check if it's a number (block number)
    if (/^\d+$/.test(trimmedSearch)) {
      navigate(`/p-chain/block/${trimmedSearch}`);
    } else {
      // Otherwise treat as transaction ID (CB58 encoded string)
      navigate(`/p-chain/tx/${trimmedSearch}`);
    }
  };

  const allFilteredSubnets = subnets
    ?.filter(subnet => {
      // Type filter
      if (typeFilter === 'l1' && subnet.subnet_type !== 'l1') return false;
      if (typeFilter === 'legacy' && subnet.subnet_type !== 'regular') return false;

      // Search filter
      return (
        subnet.name?.toLowerCase().includes(searchTerm.toLowerCase()) ||
        subnet.subnet_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
        subnet.chain_id?.toLowerCase().includes(searchTerm.toLowerCase())
      );
    })
    .sort((a, b) => b.validator_count - a.validator_count); // Sort by validator count descending

  const filteredSubnets = showAllChains ? allFilteredSubnets : allFilteredSubnets?.slice(0, 50);
  const totalChains = allFilteredSubnets?.length || 0;
  const displayedChains = filteredSubnets?.length || 0;

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
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-5 gap-4">
          {/* Total Active Chains */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-indigo-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Total Active Chains
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.active_chains || 0}
                </p>
              </div>
              <div className="p-3 bg-indigo-100 rounded-full">
                <Network size={24} className="text-indigo-600" />
              </div>
            </div>
          </div>

          {/* Active L1s */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-blue-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Active L1s
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.active_l1_subnets || 0}
                </p>
              </div>
              <div className="p-3 bg-blue-100 rounded-full">
                <Network size={24} className="text-blue-600" />
              </div>
            </div>
          </div>

          {/* Active Legacy Subnets */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-gray-400">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  Active Legacy Subnets
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : stats?.active_legacy_subnets || 0}
                </p>
              </div>
              <div className="p-3 bg-gray-100 rounded-full">
                <Network size={24} className="text-gray-600" />
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

          {/* Total L1 Validation Fees */}
          <div className="bg-white rounded-lg shadow p-6 border-l-4 border-orange-500">
            <div className="flex items-center justify-between">
              <div>
                <p className="text-sm font-semibold text-gray-600 uppercase tracking-wider">
                  L1 Validation Fees
                </p>
                <p className="text-3xl font-bold text-gray-900 mt-2">
                  {loadingStats ? '...' : `${((stats?.total_l1_fees_paid || 0) / 1e9).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 })} AVAX`}
                </p>
              </div>
              <div className="p-3 bg-orange-100 rounded-full">
                <Coins size={24} className="text-orange-600" />
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
            <h2 className="text-xl font-bold text-gray-900 mb-2">Platform Activity</h2>
            <div className="flex items-center justify-between py-3 mb-4 bg-orange-50 rounded-lg px-4 border border-orange-100">
              <span className="text-sm font-semibold text-orange-800">Transactions (7d)</span>
              <span className="text-xl font-bold text-orange-600">{loadingStats ? '...' : (stats?.recent_transactions || 0).toLocaleString()}</span>
            </div>
            <p className="text-sm text-gray-600 mb-4">Transaction breakdown (30d)</p>
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

        {/* Latest Blocks and Transactions */}
        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Latest Blocks */}
          <div className="bg-gradient-to-br from-indigo-900 via-indigo-800 to-indigo-900 rounded-lg shadow-xl overflow-hidden">
            <div className="px-6 py-4 border-b border-indigo-700">
              <h2 className="text-xl font-bold text-white">Latest Blocks</h2>
            </div>

            {loadingBlocks ? (
              <div className="p-8 text-center">
                <p className="text-indigo-300">Loading blocks...</p>
              </div>
            ) : recentBlocks && recentBlocks.length > 0 ? (
              <div className="divide-y divide-indigo-700">
                {recentBlocks.map((block) => (
                  <Link
                    key={block.block_number}
                    to={`/p-chain/block/${block.block_number}`}
                    className="block px-6 py-4 hover:bg-indigo-800/50 transition-colors"
                  >
                    <div className="flex items-center gap-4">
                      <div className="flex-shrink-0">
                        <div className="w-10 h-10 rounded-full bg-blue-500 flex items-center justify-center">
                          <span className="text-white font-bold text-sm">P</span>
                        </div>
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-white font-semibold">{block.block_number.toLocaleString()}</span>
                          <span className="text-indigo-300 text-sm">{formatTimestamp(block.block_time)}</span>
                        </div>
                        <div className="flex items-center justify-between text-sm">
                          <span className="text-indigo-300 font-mono truncate">Hash {block.block_hash}...{block.block_hash?.slice(-6)}</span>
                          <span className="text-indigo-300">{block.tx_count} {block.tx_count === 1 ? 'Tx' : 'Txs'}</span>
                        </div>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            ) : (
              <div className="p-8 text-center">
                <p className="text-indigo-300">No recent blocks found</p>
              </div>
            )}

            <div className="border-t border-indigo-700">
              <Link
                to="/p-chain/overview"
                className="block px-6 py-3 text-center text-white hover:bg-indigo-800/50 transition-colors font-medium"
              >
                View all Blocks
              </Link>
            </div>
          </div>

          {/* Latest Transactions */}
          <div className="bg-gradient-to-br from-indigo-900 via-indigo-800 to-indigo-900 rounded-lg shadow-xl overflow-hidden">
            <div className="px-6 py-4 border-b border-indigo-700">
              <h2 className="text-xl font-bold text-white mb-4">Latest Transactions</h2>

              {/* Search Bar */}
              <form onSubmit={handleSearch} className="flex gap-3">
                <div className="flex-1 relative">
                  <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 text-indigo-300" size={20} />
                  <input
                    type="text"
                    value={txBlockSearch}
                    onChange={(e) => setTxBlockSearch(e.target.value)}
                    placeholder="Search by Transaction ID or Block Number..."
                    className="w-full pl-10 pr-4 py-2 bg-indigo-800/50 border border-indigo-600 text-white placeholder-indigo-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent outline-none"
                  />
                </div>
                <button
                  type="submit"
                  className="px-6 py-2 bg-blue-600 hover:bg-blue-700 text-white font-medium rounded-lg transition-colors"
                >
                  Search
                </button>
              </form>
              <p className="text-xs text-indigo-300 mt-2">
                Enter a transaction ID (e.g., 22FdhK...) or block number (e.g., 23759061)
              </p>
            </div>

            {loadingTransactions ? (
              <div className="p-8 text-center">
                <p className="text-indigo-300">Loading transactions...</p>
              </div>
            ) : txError ? (
              <div className="p-8 text-center">
                <p className="text-red-300">Error: {String(txError)}</p>
              </div>
            ) : recentTransactions && recentTransactions.length > 0 ? (
              <div className="divide-y divide-indigo-700">
                {recentTransactions.map((tx) => (
                  <Link
                    key={tx.tx_id}
                    to={`/p-chain/tx/${tx.tx_id}`}
                    className="block px-6 py-4 hover:bg-indigo-800/50 transition-colors"
                  >
                    <div className="flex items-center gap-4">
                      <div className="flex-shrink-0">
                        <div className="w-10 h-10 rounded-full bg-blue-500 flex items-center justify-center">
                          <span className="text-white font-bold text-sm">P</span>
                        </div>
                      </div>
                      <div className="flex-1 min-w-0">
                        <div className="flex items-center justify-between mb-1">
                          <span className="text-white font-mono text-sm truncate">{tx.tx_id.substring(0, 12)}...{tx.tx_id.slice(-8)}</span>
                          <span className="text-indigo-300 text-sm">{formatTimestamp(tx.formatted_time)}</span>
                        </div>
                        <div className="flex items-center gap-2">
                          <span className="inline-flex items-center px-2.5 py-0.5 rounded text-xs font-medium bg-blue-500/20 text-blue-300 border border-blue-400/30">
                            {tx.tx_type}
                          </span>
                        </div>
                      </div>
                    </div>
                  </Link>
                ))}
              </div>
            ) : (
              <div className="p-8 text-center">
                <p className="text-indigo-300">No recent transactions found</p>
              </div>
            )}

            <div className="border-t border-indigo-700">
              <button
                className="block w-full px-6 py-3 text-center text-white hover:bg-indigo-800/50 transition-colors font-medium"
              >
                View all Transactions
              </button>
            </div>
          </div>
        </div>

        {/* All Chains Table */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <div className="flex flex-col sm:flex-row sm:items-center justify-between gap-4">
              <div>
                <h2 className="text-xl font-bold text-gray-900">All Chains</h2>
                <p className="text-sm text-gray-600 mt-1">
                  Showing {displayedChains} of {totalChains} chains (sorted by validators)
                </p>
              </div>
              <div className="flex items-center gap-3">
                {/* Type Filter */}
                <div className="flex rounded-lg border border-gray-300 overflow-hidden">
                  <button
                    onClick={() => setTypeFilter('all')}
                    className={`px-3 py-2 text-sm font-medium transition-colors ${
                      typeFilter === 'all'
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    All
                  </button>
                  <button
                    onClick={() => setTypeFilter('l1')}
                    className={`px-3 py-2 text-sm font-medium border-l border-gray-300 transition-colors ${
                      typeFilter === 'l1'
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    L1s
                  </button>
                  <button
                    onClick={() => setTypeFilter('legacy')}
                    className={`px-3 py-2 text-sm font-medium border-l border-gray-300 transition-colors ${
                      typeFilter === 'legacy'
                        ? 'bg-blue-600 text-white'
                        : 'bg-white text-gray-700 hover:bg-gray-50'
                    }`}
                  >
                    Legacy
                  </button>
                </div>
                {/* Search */}
                <div className="relative">
                  <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={18} />
                  <input
                    type="text"
                    placeholder="Search chains..."
                    value={searchTerm}
                    onChange={(e) => setSearchTerm(e.target.value)}
                    className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 w-full sm:w-64"
                  />
                </div>
              </div>
            </div>
          </div>

          {loadingSubnets ? (
            <div className="p-8 text-center">
              <p className="text-gray-500">Loading chains...</p>
            </div>
          ) : filteredSubnets && filteredSubnets.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Type
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Name
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Subnet ID
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Created Block
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Created
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Validators
                    </th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">
                      Fees Paid
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredSubnets.map((subnet, idx) => (
                    <tr key={subnet.subnet_id} className={idx % 2 === 0 ? 'bg-white' : 'bg-gray-50'}>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium ${
                          subnet.subnet_type === 'primary' ? 'bg-yellow-100 text-yellow-800 border border-yellow-300' :
                          subnet.subnet_type === 'l1' ? 'bg-blue-100 text-blue-800' :
                          subnet.subnet_type === 'elastic' ? 'bg-purple-100 text-purple-800' :
                          'bg-gray-100 text-gray-800'
                        }`}>
                          {formatSubnetType(subnet.subnet_type)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <Link
                          to={`/p-chain/subnet/${subnet.subnet_id}`}
                          className="flex items-center gap-2 hover:opacity-80 transition-opacity"
                        >
                          {subnet.logo_url && (
                            <img
                              src={subnet.logo_url}
                              alt={subnet.name || 'Subnet logo'}
                              className="w-6 h-6 rounded-full object-cover"
                              onError={(e) => { e.currentTarget.style.display = 'none'; }}
                            />
                          )}
                          <span className={`text-sm ${(subnet.name || subnet.subnet_type === 'primary') ? 'font-medium text-blue-600 hover:text-blue-800' : 'font-mono text-blue-600 hover:text-blue-800'}`}>
                            {subnet.name || (subnet.subnet_type === 'primary' ? 'Primary Network' : truncateHash(subnet.subnet_id, 10))}
                          </span>
                        </Link>
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
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm text-gray-900">
                          {subnet.created_block.toLocaleString()}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <span className="text-sm text-gray-600">
                          {formatTimestamp(subnet.created_time)}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <span className="text-sm font-semibold text-gray-900">
                          {subnet.validator_count}
                        </span>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        {subnet.subnet_type === 'l1' && subnet.total_fees_paid && subnet.total_fees_paid > 0 ? (
                          <span className="text-sm font-semibold text-orange-600">
                            {formatAVAX(subnet.total_fees_paid)} AVAX
                          </span>
                        ) : (
                          <span className="text-sm text-gray-400">-</span>
                        )}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-8 text-center">
              <p className="text-gray-500">
                {searchTerm ? 'No chains found matching your search.' : 'No chains found'}
              </p>
            </div>
          )}

          {/* Show All / Show Less Button */}
          {totalChains > 50 && filteredSubnets && filteredSubnets.length > 0 && (
            <div className="px-6 py-4 border-t border-gray-200 text-center">
              <button
                onClick={() => setShowAllChains(!showAllChains)}
                className="text-blue-600 hover:text-blue-800 text-sm font-medium transition-colors"
              >
                {showAllChains ? `Show Less` : `Show All ${totalChains} Chains`}
              </button>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default PChainOverview;
