import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo, useState } from 'react';
import { useParams, Link } from 'react-router-dom';
import PageTransition from '../components/PageTransition';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { 
  ArrowLeft, 
  Search, 
  Server, 
  Clock, 
  CheckCircle, 
  XCircle, 
  Activity,
  Wallet,
  Copy
} from 'lucide-react';

interface Validator {
  validation_id: string;
  node_id: string;
  weight: string;
  balance: string;
  start_time: string;
  end_time: string;
  uptime_percentage: number;
  active: boolean;
  last_updated: string;
}

interface SubnetDetails {
  subnet_id: string;
  chain_id: string;
  conversion_block: number;
  conversion_time: string;
  validator_count: number;
  total_weight: string;
}

function SubnetValidators() {
  const { subnetId } = useParams<{ subnetId: string }>();
  const { url } = useClickhouseUrl();
  const [searchTerm, setSearchTerm] = useState('');

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  // Subnet Details
  const { data: subnetDetails, isLoading: loadingDetails } = useQuery<SubnetDetails>({
    queryKey: ['subnet-details', subnetId, url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            subnet_id,
            chain_id,
            conversion_block,
            formatDateTime(conversion_time, '%Y-%m-%d %H:%i:%s') as conversion_time,
            (SELECT count(*) FROM l1_validator_state WHERE subnet_id = l1_subnets.subnet_id AND active = true) as validator_count,
            (SELECT toString(sum(weight)) FROM l1_validator_state WHERE subnet_id = l1_subnets.subnet_id AND active = true) as total_weight
          FROM l1_subnets
          WHERE subnet_id = '${subnetId}'
          LIMIT 1
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<SubnetDetails>();
      return (data as SubnetDetails[])[0];
    },
  });

  // Validators List
  const { data: validators, isLoading: loadingValidators } = useQuery<Validator[]>({
    queryKey: ['subnet-validators', subnetId, url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            validation_id,
            node_id,
            toString(weight) as weight,
            toString(balance) as balance,
            formatDateTime(start_time, '%Y-%m-%d %H:%i:%s') as start_time,
            formatDateTime(end_time, '%Y-%m-%d %H:%i:%s') as end_time,
            uptime_percentage,
            active,
            formatDateTime(last_updated, '%Y-%m-%d %H:%i:%s') as last_updated
          FROM l1_validator_state
          WHERE subnet_id = '${subnetId}'
          ORDER BY weight DESC
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<Validator>();
      return data as Validator[];
    },
    refetchInterval: 30000,
  });

  const formatWeight = (weight: string) => {
    const num = parseFloat(weight);
    if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`;
    if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`;
    if (num >= 1e3) return `${(num / 1e3).toFixed(2)}K`;
    return num.toFixed(0);
  };

  const formatBalance = (balance: string) => {
    const num = parseFloat(balance);
    return (num / 1e9).toLocaleString(undefined, { maximumFractionDigits: 2 }) + ' AVAX';
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const filteredValidators = validators?.filter(v => 
    v.node_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
    v.validation_id.toLowerCase().includes(searchTerm.toLowerCase())
  );

  if (loadingDetails) {
    return (
      <div className="p-8 flex items-center justify-center min-h-[400px]">
        <p className="text-gray-500">Loading subnet details...</p>
      </div>
    );
  }

  if (!subnetDetails) {
    return (
      <div className="p-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900">Subnet Not Found</h2>
        <Link to="/p-chain/overview" className="text-blue-600 hover:text-blue-800 mt-4 inline-block">
          ← Back to Overview
        </Link>
      </div>
    );
  }

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        {/* Header with Back Button */}
        <div>
          <Link 
            to="/p-chain/overview" 
            className="inline-flex items-center gap-2 text-gray-600 hover:text-gray-900 mb-4 transition-colors"
          >
            <ArrowLeft size={20} />
            Back to Overview
          </Link>
          
          <div className="bg-white rounded-lg shadow-sm border border-gray-200 p-6">
            <div className="flex items-start justify-between">
              <div>
                <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
                  Subnet Details
                  <span className="px-3 py-1 bg-blue-100 text-blue-800 text-sm font-medium rounded-full">
                    L1
                  </span>
                </h1>
                <div className="mt-2 space-y-1">
                  <p className="text-sm text-gray-500 font-mono">ID: {subnetDetails.subnet_id}</p>
                  {subnetDetails.chain_id && (
                    <p className="text-sm text-gray-500 font-mono">Chain ID: {subnetDetails.chain_id}</p>
                  )}
                </div>
              </div>
              
              <div className="flex gap-6 text-right">
                <div>
                  <p className="text-sm text-gray-500 uppercase tracking-wide font-semibold">Validators</p>
                  <p className="text-2xl font-bold text-gray-900">{subnetDetails.validator_count}</p>
                </div>
                <div>
                  <p className="text-sm text-gray-500 uppercase tracking-wide font-semibold">Total Weight</p>
                  <p className="text-2xl font-bold text-gray-900">{formatWeight(subnetDetails.total_weight)}</p>
                </div>
              </div>
            </div>

            <div className="mt-6 grid grid-cols-1 md:grid-cols-3 gap-4 pt-6 border-t border-gray-100">
              <div className="flex items-center gap-3 text-sm text-gray-600">
                <Server size={18} className="text-gray-400" />
                <span>Converted at block <strong>{subnetDetails.conversion_block.toLocaleString()}</strong></span>
              </div>
              <div className="flex items-center gap-3 text-sm text-gray-600">
                <Clock size={18} className="text-gray-400" />
                <span>Converted on <strong>{new Date(subnetDetails.conversion_time).toLocaleDateString()}</strong></span>
              </div>
            </div>
          </div>
        </div>

        {/* Validators List */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200 flex flex-col sm:flex-row sm:items-center justify-between gap-4">
            <h2 className="text-lg font-bold text-gray-900">Validators</h2>
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 text-gray-400" size={18} />
              <input
                type="text"
                placeholder="Search Node ID..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 pr-4 py-2 border border-gray-300 rounded-lg focus:outline-none focus:ring-2 focus:ring-blue-500 focus:border-blue-500 w-full sm:w-64"
              />
            </div>
          </div>

          {loadingValidators ? (
            <div className="p-12 text-center">
              <p className="text-gray-500">Loading validators...</p>
            </div>
          ) : filteredValidators && filteredValidators.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Node ID</th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Status</th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">Weight</th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">Balance</th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Duration</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredValidators.map((validator) => (
                    <tr key={validator.validation_id} className="hover:bg-gray-50 transition-colors">
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex flex-col gap-1">
                          <div className="flex items-center gap-2">
                            <span className="text-sm font-medium text-gray-900 font-mono">{validator.node_id}</span>
                            <button
                              onClick={() => copyToClipboard(validator.node_id)}
                              className="text-gray-400 hover:text-gray-600 transition-colors"
                              title="Copy Node ID"
                            >
                              <Copy size={12} />
                            </button>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="text-xs text-gray-500 font-mono" title="Validation ID">
                              {validator.validation_id.substring(0, 12)}...
                            </span>
                            <button
                              onClick={() => copyToClipboard(validator.validation_id)}
                              className="text-gray-400 hover:text-gray-600 transition-colors"
                              title="Copy Validation ID"
                            >
                              <Copy size={12} />
                            </button>
                          </div>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          {validator.active ? (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                              <CheckCircle size={12} /> Active
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-gray-100 text-gray-800">
                              <XCircle size={12} /> Inactive
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Activity size={16} className="text-gray-400" />
                          <span className="text-sm font-semibold text-gray-900">{formatWeight(validator.weight)}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Wallet size={16} className="text-gray-400" />
                          <span className="text-sm text-gray-900">{formatBalance(validator.balance)}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-xs text-gray-500 space-y-1">
                          <p>Start: {new Date(validator.start_time).toLocaleDateString()}</p>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-12 text-center">
              <p className="text-gray-500">No validators found matching your search.</p>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default SubnetValidators;
