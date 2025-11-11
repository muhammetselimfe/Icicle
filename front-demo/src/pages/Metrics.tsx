import { useQuery } from '@tanstack/react-query';
import { useParams, useNavigate } from 'react-router-dom';
import { createClient } from '@clickhouse/client-web';
import { useState, useEffect } from 'react';
import PageTransition from '../components/PageTransition';
import MetricChart from '../components/MetricChart';

const clickhouse = createClient({
  url: 'http://localhost:8123',
  username: "anonymous",
});

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

type Granularity = 'hour' | 'day' | 'week' | 'month';

const GRANULARITIES: { value: Granularity; label: string }[] = [
  { value: 'hour', label: 'Hour' },
  { value: 'day', label: 'Day' },
  { value: 'week', label: 'Week' },
  { value: 'month', label: 'Month' },
];

function Metrics() {
  const { chainId, granularity } = useParams<{ chainId: string; granularity: string }>();
  const navigate = useNavigate();
  const [metricNames, setMetricNames] = useState<string[]>([]);
  const [loadingMetricsList, setLoadingMetricsList] = useState(false);

  const selectedChainId = chainId ? parseInt(chainId) : 43114;
  const selectedGranularity = (granularity as Granularity) || 'hour';

  // Cache chains for 5 minutes
  const { data: chains, isLoading, error } = useQuery<Chain[]>({
    queryKey: ['chains'],
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
                  WHERE chain_id = ${selectedChainId} AND granularity = '${selectedGranularity}'
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
  }, [selectedChainId, selectedGranularity]);

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        <h1 className="text-3xl font-bold text-gray-900">Metrics</h1>

        <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
          {/* Chain Selector */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Chain</h2>

            {isLoading && (
              <p className="text-gray-500">Loading chains...</p>
            )}

            {error && (
              <p className="text-red-600">Error loading chains: {error.message}</p>
            )}

            {chains && (
              <div className="space-y-2">
                {chains.map((chain) => {
                  const isSelected = selectedChainId === chain.chain_id;
                  return (
                    <button
                      key={chain.chain_id}
                      onClick={() => navigate(`/metrics/${chain.chain_id}/${selectedGranularity}`)}
                      className={`w-full flex items-center gap-3 p-3 border rounded-lg transition-all text-left cursor-pointer ${isSelected
                        ? 'border-blue-500 bg-blue-50 ring-2 ring-blue-200'
                        : 'border-gray-200 hover:bg-gray-50 hover:border-gray-300'
                        }`}
                    >
                      <div className="flex-1">
                        <div className={`font-medium ${isSelected ? 'text-blue-900' : 'text-gray-900'}`}>
                          {chain.name}
                        </div>
                        <div className={`text-sm ${isSelected ? 'text-blue-600' : 'text-gray-500'}`}>
                          Chain ID: {chain.chain_id}
                        </div>
                      </div>
                      {isSelected && (
                        <div className="w-2 h-2 rounded-full bg-blue-500" />
                      )}
                    </button>
                  );
                })}
              </div>
            )}
          </div>

          {/* Granularity Selector */}
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-lg font-semibold text-gray-900 mb-4">Granularity</h2>
            <div className="grid grid-cols-2 gap-2">
              {GRANULARITIES.map((granularity) => {
                const isSelected = selectedGranularity === granularity.value;
                return (
                  <button
                    key={granularity.value}
                    onClick={() => navigate(`/metrics/${selectedChainId}/${granularity.value}`)}
                    className={`p-3 border rounded-lg transition-all font-medium cursor-pointer ${isSelected
                      ? 'border-blue-500 bg-blue-50 text-blue-900 ring-2 ring-blue-200'
                      : 'border-gray-200 hover:bg-gray-50 hover:border-gray-300 text-gray-700'
                      }`}
                  >
                    {granularity.label}
                  </button>
                );
              })}
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
                granularity={selectedGranularity}
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
  granularity
}: {
  metricName: string;
  chainId: number;
  granularity: string;
}) {
  const { data, isLoading, error } = useQuery<MetricQueryResult>({
    queryKey: ['metric', chainId, granularity, metricName],
    queryFn: async () => {
      const sqlQuery = `SELECT chain_id, metric_name, granularity, period, value, computed_at 
                FROM metrics 
                WHERE chain_id = ${chainId} 
                  AND granularity = '${granularity}'
                  AND metric_name = '${metricName}'
                ORDER BY period ASC`;

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
        granularity={granularity}
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

