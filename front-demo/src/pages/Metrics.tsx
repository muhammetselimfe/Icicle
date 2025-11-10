import { useQuery } from '@tanstack/react-query';
import { useParams, useNavigate } from 'react-router-dom';
import { createClient } from '@clickhouse/client-web';
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

  const selectedChainId = chainId ? parseInt(chainId) : 43114;
  const selectedGranularity = (granularity as Granularity) || 'hour';

  const { data: chains, isLoading, error } = useQuery<Chain[]>({
    queryKey: ['chains'],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: 'SELECT chain_id, name FROM chain_status',
        format: 'JSONEachRow',
      });
      const data = await result.json<Chain>();
      return data as Chain[];
    },
    staleTime: Infinity,
    gcTime: Infinity,
  });

  const { data: metrics, isLoading: isLoadingMetrics, error: metricsError } = useQuery<MetricData[]>({
    queryKey: ['metrics', selectedChainId, selectedGranularity],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `SELECT chain_id, metric_name, granularity, period, value, computed_at 
                FROM metrics 
                WHERE chain_id = ${selectedChainId} AND granularity = '${selectedGranularity}'
                ORDER BY period ASC`,
        format: 'JSONEachRow',
      });
      const data = await result.json<MetricData>();
      return data as MetricData[];
    },
  });

  const metricsByName = metrics?.reduce((acc, metric) => {
    if (!acc[metric.metric_name]) {
      acc[metric.metric_name] = [];
    }
    acc[metric.metric_name].push(metric);
    return acc;
  }, {} as Record<string, MetricData[]>) || {};

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
            {isLoadingMetrics && (
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">Loading metrics...</p>
              </div>
            )}

            {metricsError && (
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-red-600">Error loading metrics: {metricsError.message}</p>
              </div>
            )}

            {metrics && Object.keys(metricsByName).length === 0 && (
              <div className="bg-white rounded-lg shadow p-6">
                <p className="text-gray-500">No metrics data available for this chain and granularity.</p>
              </div>
            )}

            {Object.entries(metricsByName).map(([metricName, data]) => (
              <MetricChart
                key={metricName}
                metricName={metricName}
                data={data}
                granularity={selectedGranularity}
              />
            ))}
          </div>
        )}
      </div>
    </PageTransition>
  );
}

export default Metrics;

