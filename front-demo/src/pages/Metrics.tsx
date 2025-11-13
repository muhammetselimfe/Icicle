import { useQuery } from '@tanstack/react-query';
import { useParams, useNavigate } from 'react-router-dom';
import { createClient } from '@clickhouse/client-web';
import { useState, useEffect, useMemo } from 'react';
import PageTransition from '../components/PageTransition';
import MetricChart from '../components/MetricChart';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { useSyncStatus } from '../hooks/useSyncStatus';

interface Chain {
  chain_id: number;
  name: string;
}

interface MetricData {
  chain_id: number;
  metric_name: string;
  granularity: string;
  period: string;
  value: number;
  computed_at: string;
}

type TimePeriod = '24h' | '7d' | '30d' | '90d' | '1y';

interface TimePeriodConfig {
  value: TimePeriod;
  label: string;
  granularity: 'hour' | 'day' | 'week';
  clickhouseInterval: string; // e.g., "24 HOUR", "7 DAY"
}

const TIME_PERIODS: TimePeriodConfig[] = [
  { value: '24h', label: '24 Hours', granularity: 'hour', clickhouseInterval: '24 HOUR' },
  { value: '7d', label: '7 Days', granularity: 'hour', clickhouseInterval: '7 DAY' },
  { value: '30d', label: '30 Days', granularity: 'day', clickhouseInterval: '30 DAY' },
  { value: '90d', label: '90 Days', granularity: 'day', clickhouseInterval: '90 DAY' },
  { value: '1y', label: '1 Year', granularity: 'day', clickhouseInterval: '1 YEAR' },
];

function Metrics() {
  const { chainId, timePeriod } = useParams<{ chainId: string; timePeriod: string }>();
  const navigate = useNavigate();
  const [metricNames, setMetricNames] = useState<string[]>([]);
  const [loadingMetricsList, setLoadingMetricsList] = useState(false);
  const { url } = useClickhouseUrl();
  const { chains: syncStatusChains } = useSyncStatus();

  const selectedChainId = chainId ? parseInt(chainId) : 43114;
  const selectedTimePeriod = (timePeriod as TimePeriod) || '7d';
  const periodConfig = TIME_PERIODS.find(p => p.value === selectedTimePeriod) || TIME_PERIODS[1];

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  // Cache chains for 5 minutes
  const { data: chains, isLoading, error } = useQuery<Chain[]>({
    queryKey: ['chains', url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: 'SELECT chain_id, name FROM chain_status FINAL WHERE chain_id !=0 ',
        format: 'JSONEachRow',
      });
      const data = await result.json<Chain>();
      return data as Chain[];
    },
    staleTime: 5 * 60 * 1000, // 5 minutes
    gcTime: 10 * 60 * 1000, // 10 minutes
  });

  // Fetch list of available metric names
  useEffect(() => {
    let cancelled = false;

    async function fetchMetricNames() {
      setLoadingMetricsList(true);
      setMetricNames([]);

      try {
        const result = await clickhouse.query({
          query: `SELECT DISTINCT metric_name 
                  FROM metrics 
                  WHERE chain_id = ${selectedChainId} AND granularity = '${periodConfig.granularity}'
                  ORDER BY metric_name`,
          format: 'JSONEachRow',
        });
        const data = await result.json<{ metric_name: string }>();

        if (!cancelled) {
          const names = (data as { metric_name: string }[]).map(d => d.metric_name);

          // Stagger the metric names to trigger progressive loading
          names.forEach((name, index) => {
            setTimeout(() => {
              if (!cancelled) {
                setMetricNames(prev => [...prev, name]);
              }
            }, index * 10);
          });
        }
      } catch (err) {
        console.error('Error fetching metric names:', err);
      } finally {
        setLoadingMetricsList(false);
      }
    }

    fetchMetricNames();

    return () => {
      cancelled = true;
    };
  }, [selectedChainId, periodConfig.granularity, clickhouse]);

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        <div className="flex items-center justify-between">
          <h1 className="text-3xl font-bold text-gray-900">EVM Metrics</h1>
        </div>

        {/* Compact filters */}
        <div className="bg-white rounded-lg shadow p-4">
          <div className="flex flex-col lg:flex-row gap-4 items-start lg:items-center">
            {/* Chain Selector */}
            <div className="flex items-center gap-3 min-w-0 flex-shrink-0">
              <label className="text-sm font-semibold text-gray-700 whitespace-nowrap">Chain:</label>
              {isLoading && (
                <p className="text-sm text-gray-500">Loading...</p>
              )}
              {error && (
                <p className="text-sm text-red-600">Error loading chains</p>
              )}
              {chains && (
                <select
                  value={selectedChainId}
                  onChange={(e) => navigate(`/evm-metrics/${e.target.value}/${selectedTimePeriod}`)}
                  className="px-3 py-2 border border-gray-300 rounded-lg text-sm font-medium text-gray-900 bg-white hover:bg-gray-50 focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 cursor-pointer"
                >
                  {chains.map((chain) => {
                    const syncStatus = syncStatusChains?.find(c => c.chain_id === chain.chain_id);
                    const needsWarning = syncStatus && syncStatus.syncPercentage < 99.9;
                    return (
                      <option key={chain.chain_id} value={chain.chain_id}>
                        {chain.name} (ID: {chain.chain_id})
                        {needsWarning ? ` - ${syncStatus.syncPercentage.toFixed(1)}% synced` : ''}
                      </option>
                    );
                  })}
                </select>
              )}
            </div>

            {/* Time Period Pills */}
            <div className="flex items-center gap-3 flex-wrap flex-1">
              <label className="text-sm font-semibold text-gray-700 whitespace-nowrap">Period:</label>
              <div className="flex gap-2 flex-wrap">
                {TIME_PERIODS.map((period) => {
                  const isSelected = selectedTimePeriod === period.value;
                  return (
                    <button
                      key={period.value}
                      onClick={() => navigate(`/evm-metrics/${selectedChainId}/${period.value}`)}
                      className={`px-4 py-1.5 rounded-full text-sm font-medium transition-all cursor-pointer ${isSelected
                        ? 'bg-blue-500 text-white shadow-sm'
                        : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                    >
                      {period.label}
                    </button>
                  );
                })}
              </div>
            </div>
          </div>
        </div>

        {/* Metrics Charts */}
        {selectedChainId && (
          <div className="space-y-6">
            {loadingMetricsList && metricNames.length === 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">Loading metrics...</p>
              </div>
            )}

            {!loadingMetricsList && metricNames.length === 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">No metrics data available for this chain and granularity.</p>
              </div>
            )}

            {metricNames.map((metricName) => (
              <MetricChartLoader
                key={metricName}
                metricName={metricName}
                chainId={selectedChainId}
                periodConfig={periodConfig}
                clickhouse={clickhouse}
                url={url}
              />
            ))}
          </div>
        )}
      </div>
    </PageTransition>
  );
}

interface MetricQueryResult {
  data: MetricData[];
  executionTimeMs?: number;
  rowsRead?: number;
  bytesRead?: number;
  query: string;
}

// Separate component to load each metric independently
function MetricChartLoader({
  metricName,
  chainId,
  periodConfig,
  clickhouse,
  url
}: {
  metricName: string;
  chainId: number;
  periodConfig: TimePeriodConfig;
  clickhouse: ReturnType<typeof createClient>;
  url: string;
}) {
  const { data, isLoading, error } = useQuery<MetricQueryResult>({
    queryKey: ['metric', chainId, periodConfig.value, metricName, url],
    queryFn: async () => {
      let sqlQuery: string;


      // Calculate step size and truncation function based on granularity
      let stepInterval: string;
      let truncateFunc: string;
      if (periodConfig.granularity === 'hour') {
        stepInterval = 'INTERVAL 1 HOUR';
        truncateFunc = 'toStartOfHour';
      } else if (periodConfig.granularity === 'day') {
        stepInterval = 'INTERVAL 1 DAY';
        truncateFunc = 'toStartOfDay';
      } else {
        stepInterval = 'INTERVAL 1 WEEK';
        truncateFunc = 'toStartOfWeek';
      }

      // Use WITH FILL - simpler approach
      // Select period and value, then add the constants
      sqlQuery = `
          SELECT 
            ${chainId} as chain_id,
            '${metricName}' as metric_name,
            '${periodConfig.granularity}' as granularity,
            period,
            if(value IS NULL, 0, value) as value,
            if(computed_at IS NULL, toDateTime64(now(), 3, 'UTC'), computed_at) as computed_at
          FROM (
            SELECT 
              period,
              any(value) as value,
              any(computed_at) as computed_at
            FROM metrics 
            WHERE chain_id = ${chainId} 
              AND granularity = '${periodConfig.granularity}' 
              AND metric_name = '${metricName}'
              AND period >= toDateTime64(${truncateFunc}(now() - INTERVAL ${periodConfig.clickhouseInterval}), 3, 'UTC')
            GROUP BY period
            ORDER BY period ASC WITH FILL 
              FROM toDateTime64(${truncateFunc}(now() - INTERVAL ${periodConfig.clickhouseInterval}), 3, 'UTC') 
              TO toDateTime64(${truncateFunc}(now()) + ${stepInterval}, 3, 'UTC')
              STEP ${stepInterval}
          )`;

      const result = await clickhouse.query({
        query: sqlQuery,
        format: 'JSONEachRow',
      });
      const metricData = await result.json<MetricData>();

      // Extract execution stats from ClickHouse summary header
      const summaryHeader = result.response_headers['x-clickhouse-summary'];
      let executionTimeMs: number | undefined;
      let rowsRead: number | undefined;
      let bytesRead: number | undefined;

      if (summaryHeader && typeof summaryHeader === 'string') {
        try {
          const summary = JSON.parse(summaryHeader);
          const elapsedNs = summary?.elapsed_ns ? parseInt(summary.elapsed_ns) : undefined;
          executionTimeMs = elapsedNs ? elapsedNs / 1_000_000 : undefined;
          rowsRead = summary?.read_rows ? parseInt(summary.read_rows) : undefined;
          bytesRead = summary?.read_bytes ? parseInt(summary.read_bytes) : undefined;
        } catch (e) {
          console.error('Failed to parse ClickHouse summary:', e);
        }
      }

      return {
        data: metricData as MetricData[],
        executionTimeMs,
        rowsRead,
        bytesRead,
        query: sqlQuery,
      };
    },
    staleTime: 30 * 1000, // 30 seconds
  });

  if (isLoading) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <div className="animate-pulse">
          <div className="h-4 bg-gray-200 rounded w-1/4 mb-4"></div>
          <div className="h-64 bg-gray-100 rounded"></div>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="bg-white rounded-lg shadow p-6">
        <p className="text-red-600">Error loading {metricName}: {error.message}</p>
      </div>
    );
  }

  if (!data || data.data.length === 0) {
    return null;
  }

  return (
    <div className="bg-white rounded-lg shadow overflow-hidden">
      <MetricChart
        metricName={metricName}
        data={data.data}
        granularity={periodConfig.granularity}
      />

      {/* Query Stats */}
      <div className="px-6 pb-4 space-y-2 border-t border-gray-100 bg-gray-50">
        <div className="flex items-center justify-between pt-3">
          {data.executionTimeMs !== undefined && (
            <div className="flex items-center gap-1.5 text-xs text-gray-600">
              <span>⚡</span>
              <span>Query time:</span>
              <span className="font-semibold text-gray-900">
                {(data.executionTimeMs / 1000).toFixed(3)}s
              </span>
            </div>
          )}

          {/* SQL Query */}
          <details className="group">
            <summary className="text-xs text-gray-500 hover:text-gray-700 cursor-pointer select-none">
              Show SQL query
            </summary>
            <pre className="mt-2 p-3 bg-gray-800 text-gray-100 rounded text-xs overflow-x-auto font-mono">
              {data.query}
            </pre>
          </details>
        </div>
      </div>
    </div>
  );
}

export default Metrics;

