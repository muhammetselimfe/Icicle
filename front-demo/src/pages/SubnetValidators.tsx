import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo, useState } from 'react';
import { useParams, Link, useNavigate } from 'react-router-dom';
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
  Copy,
  ExternalLink,
  Globe,
  ArrowUpDown,
  ArrowUp,
  ArrowDown,
  ChevronRight,
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
  // Historical fields
  created_tx_type?: string;
  created_tx_id?: string;
  created_block?: number;
  created_time?: string;
  initial_balance?: string;
  initial_weight?: string;
  bls_public_key?: string;
  // Fee tracking fields
  initial_deposit?: string;
  total_topups?: string;
  refund_amount?: string;
  fees_paid?: string;
  daily_burn_rate?: number;
  days_until_empty?: number;
}

interface SubnetDetails {
  subnet_id: string;
  subnet_type: string;
  chain_id: string;
  conversion_block: number;
  conversion_time: string;
  validator_count: number;
  total_weight: string;
  name?: string;
  description?: string;
  logo_url?: string;
  website_url?: string;
}

function SubnetValidators() {
  const { subnetId } = useParams<{ subnetId: string }>();
  const { url } = useClickhouseUrl();
  const navigate = useNavigate();
  const [searchTerm, setSearchTerm] = useState('');
  const [showAll, setShowAll] = useState(false);
  const [sortOrder, setSortOrder] = useState<'asc' | 'desc' | null>('desc');

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
            s.subnet_id,
            s.subnet_type,
            s.chain_id,
            s.converted_block as conversion_block,
            formatDateTime(s.converted_time, '%Y-%m-%d %H:%i:%s') as conversion_time,
            (SELECT count(DISTINCT node_id) FROM l1_validator_history FINAL WHERE subnet_id = {subnetId:String} AND length(node_id) > 30) as validator_count,
            (SELECT toString(sum(weight)) FROM l1_validator_state FINAL WHERE subnet_id = {subnetId:String} AND length(node_id) > 30 AND active = true) as total_weight,
            NULLIF(r.name, '') as name,
            NULLIF(r.description, '') as description,
            NULLIF(r.logo_url, '') as logo_url,
            NULLIF(r.website_url, '') as website_url
          FROM subnets AS s FINAL
          LEFT JOIN l1_registry AS r FINAL ON s.subnet_id = r.subnet_id
          WHERE s.subnet_id = {subnetId:String}
          LIMIT 1
        `,
        format: 'JSONEachRow',
        query_params: { subnetId },
      });
      const data = await result.json<SubnetDetails>();
      return (data as SubnetDetails[])[0];
    },
  });

  // Validators List with ALL available data
  const { data: validators, isLoading: loadingValidators, error: validatorsError } = useQuery<Validator[]>({
    queryKey: ['subnet-validators', subnetId, subnetDetails?.subnet_type, url],
    queryFn: async () => {
      try {
        console.log('[Validators Query] Starting query for subnet:', subnetId, 'type:', subnetDetails?.subnet_type);
        // For Primary Network and legacy subnets, show stake (weight) instead of balance
        if (subnetDetails?.subnet_type === 'primary' || subnetDetails?.subnet_type === 'regular') {
        const result = await clickhouse.query({
          query: `
            SELECT
              validation_id,
              node_id,
              toString(weight) as weight,
              toString(weight) as balance,
              formatDateTime(start_time, '%Y-%m-%d %H:%i:%s') as start_time,
              formatDateTime(end_time, '%Y-%m-%d %H:%i:%s') as end_time,
              uptime_percentage,
              if(active, true, false) as active,
              formatDateTime(last_updated, '%Y-%m-%d %H:%i:%s') as last_updated
            FROM l1_validator_state FINAL
            WHERE subnet_id = {subnetId:String}
            ORDER BY weight DESC
          `,
          format: 'JSONEachRow',
          query_params: { subnetId },
        });
        console.log('[Validators Query] Primary/Regular query executed');
        const data = await result.json<Validator[]>();
        console.log('[Validators Query] Parsed data:', data?.length, 'validators');
        return data;
      }

      // For L1 subnets, show ALL validators with ALL available data
      // Simplified query without expensive p_chain_txs join
      console.log('[Validators Query] Running L1 query for subnet:', subnetId);
      const result = await clickhouse.query({
        query: `
          WITH
          -- Current validator state (from RPC getCurrentValidators)
          current_state AS (
            SELECT
              node_id,
              validation_id,
              weight,
              balance,
              start_time,
              end_time,
              uptime_percentage,
              active,
              last_updated
            FROM l1_validator_state FINAL
            WHERE subnet_id = {subnetId:String}
              AND length(node_id) > 0
          ),
          -- Historical validators (from transaction parsing)
          history AS (
            SELECT
              node_id,
              created_tx_type,
              created_tx_id,
              created_block,
              created_time,
              initial_balance,
              initial_weight,
              bls_public_key
            FROM l1_validator_history FINAL
            WHERE subnet_id = {subnetId:String}
              AND length(node_id) > 0
          ),
          -- Current validators with their history (if available)
          current_with_history AS (
            SELECT
              cs.validation_id as validation_id,
              cs.node_id as node_id,
              toString(cs.weight) as weight,
              toString(cs.balance) as balance,
              formatDateTime(cs.start_time, '%Y-%m-%d %H:%i:%s') as start_time,
              formatDateTime(cs.end_time, '%Y-%m-%d %H:%i:%s') as end_time,
              cs.uptime_percentage as uptime_percentage,
              cs.active as active,
              formatDateTime(cs.last_updated, '%Y-%m-%d %H:%i:%s') as last_updated,
              h.created_tx_type as created_tx_type,
              h.created_tx_id as created_tx_id,
              h.created_block as created_block,
              formatDateTime(h.created_time, '%Y-%m-%d %H:%i:%s') as created_time,
              toString(h.initial_balance) as initial_balance,
              toString(h.initial_weight) as initial_weight,
              h.bls_public_key as bls_public_key,
              -- Fee tracking: use initial_balance from history as initial_deposit
              toString(COALESCE(h.initial_balance, 0)) as initial_deposit,
              '0' as total_topups,
              -- Refund amount is current balance (what would be returned)
              toString(cs.balance) as refund_amount,
              -- Fees paid = initial_deposit - current_balance (simplified without topups)
              toString(
                CASE
                  WHEN COALESCE(h.initial_balance, 0) > cs.balance
                  THEN COALESCE(h.initial_balance, 0) - cs.balance
                  ELSE 0
                END
              ) as fees_paid,
              -- Daily burn rate (approximate based on age)
              CASE
                WHEN dateDiff('day', cs.start_time, now()) > 0 AND COALESCE(h.initial_balance, 0) > cs.balance
                THEN toFloat64(COALESCE(h.initial_balance, 0) - cs.balance) / dateDiff('day', cs.start_time, now())
                ELSE 0
              END as daily_burn_rate,
              -- Days until empty
              CASE
                WHEN dateDiff('day', cs.start_time, now()) > 0 AND COALESCE(h.initial_balance, 0) > cs.balance
                THEN cs.balance / (toFloat64(COALESCE(h.initial_balance, 0) - cs.balance) / dateDiff('day', cs.start_time, now()))
                ELSE 0
              END as days_until_empty
            FROM current_state cs
            LEFT JOIN history h ON cs.node_id = h.node_id
          ),
          -- Historical validators not in current state (inactive)
          history_only AS (
            SELECT
              '' as validation_id,
              h.node_id as node_id,
              toString(h.initial_weight) as weight,
              '0' as balance,
              formatDateTime(h.created_time, '%Y-%m-%d %H:%i:%s') as start_time,
              '1970-01-01 00:00:00' as end_time,
              0 as uptime_percentage,
              false as active,
              formatDateTime(h.created_time, '%Y-%m-%d %H:%i:%s') as last_updated,
              h.created_tx_type as created_tx_type,
              h.created_tx_id as created_tx_id,
              h.created_block as created_block,
              formatDateTime(h.created_time, '%Y-%m-%d %H:%i:%s') as created_time,
              toString(h.initial_balance) as initial_balance,
              toString(h.initial_weight) as initial_weight,
              h.bls_public_key as bls_public_key,
              toString(h.initial_balance) as initial_deposit,
              '0' as total_topups,
              '0' as refund_amount,
              toString(h.initial_balance) as fees_paid,
              0 as daily_burn_rate,
              0 as days_until_empty
            FROM history h
            WHERE h.node_id NOT IN (SELECT node_id FROM current_state)
          )
          -- Combine both sets
          SELECT * FROM current_with_history
          UNION ALL
          SELECT * FROM history_only
          ORDER BY active DESC, toUInt64OrZero(weight) DESC
        `,
        format: 'JSONEachRow',
        query_params: { subnetId },
      });
      console.log('[Validators Query] Query executed, parsing JSON...');
      const data = await result.json<Validator[]>();
      console.log('[Validators Query] Parsed data:', data?.length, 'validators');
      return data;
      } catch (error) {
        console.error('[Validators Query] Error:', error);
        throw error;
      }
    },
    refetchInterval: 30000,
    enabled: !!subnetDetails,
  });

  // Debug logging for validators error
  if (validatorsError) {
    console.error('[Validators Query] Error:', validatorsError);
  }

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

  const formatSubnetType = (type: string) => {
    switch (type) {
      case 'l1': return 'L1';
      case 'regular': return 'Legacy Subnet';
      case 'elastic': return 'Elastic';
      case 'primary': return 'Primary';
      default: return type;
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const allFilteredValidators = validators?.filter(v =>
    v.node_id.toLowerCase().includes(searchTerm.toLowerCase()) ||
    v.validation_id.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const sortedValidators = allFilteredValidators?.slice().sort((a, b) => {
    if (!sortOrder) return 0;
    const aBalance = parseFloat(a.balance);
    const bBalance = parseFloat(b.balance);
    return sortOrder === 'desc' ? bBalance - aBalance : aBalance - bBalance;
  });

  const filteredValidators = showAll ? sortedValidators : sortedValidators?.slice(0, 10);
  const totalValidators = allFilteredValidators?.length || 0;
  const displayedValidators = filteredValidators?.length || 0;

  const handleSortToggle = () => {
    setSortOrder(current => {
      if (current === 'desc') return 'asc';
      if (current === 'asc') return null;
      return 'desc';
    });
  };

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
              <div className="flex-1">
                <div className="flex items-start gap-4">
                  {subnetDetails.logo_url && (
                    <img
                      src={subnetDetails.logo_url}
                      alt={subnetDetails.name || 'Subnet logo'}
                      className="w-16 h-16 rounded-lg object-cover border border-gray-200"
                      onError={(e) => { e.currentTarget.style.display = 'none'; }}
                    />
                  )}
                  <div className="flex-1">
                    <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
                      {subnetDetails.name || (subnetDetails.subnet_type === 'primary' ? 'Primary Network' : 'Subnet Details')}
                      <span className={`px-3 py-1 text-sm font-medium rounded-full ${
                        subnetDetails.subnet_type === 'primary' ? 'bg-yellow-100 text-yellow-800 border border-yellow-300' :
                        subnetDetails.subnet_type === 'l1' ? 'bg-blue-100 text-blue-800' :
                        subnetDetails.subnet_type === 'elastic' ? 'bg-purple-100 text-purple-800' :
                        'bg-gray-100 text-gray-800'
                      }`}>
                        {formatSubnetType(subnetDetails.subnet_type)}
                      </span>
                    </h1>
                    {subnetDetails.description && (
                      <p className="mt-2 text-sm text-gray-600 max-w-2xl">{subnetDetails.description}</p>
                    )}
                    <div className="mt-3 space-y-1">
                      <p className="text-sm text-gray-500 font-mono">Subnet ID: {subnetDetails.subnet_id}</p>
                      {subnetDetails.chain_id && (
                        <p className="text-sm text-gray-500 font-mono">Chain ID: {subnetDetails.chain_id}</p>
                      )}
                      {subnetDetails.website_url && (
                        <a
                          href={subnetDetails.website_url}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="inline-flex items-center gap-1 text-sm text-blue-600 hover:text-blue-800 transition-colors"
                        >
                          <Globe size={14} />
                          {subnetDetails.website_url.replace(/^https?:\/\//, '')}
                          <ExternalLink size={12} />
                        </a>
                      )}
                    </div>
                  </div>
                </div>
              </div>

              <div className="flex gap-6 text-right ml-6">
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
            <div>
              <h2 className="text-lg font-bold text-gray-900">Validators</h2>
              <p className="text-sm text-gray-600 mt-1">
                Showing {displayedValidators} of {totalValidators} validators
                <span className="text-gray-400 ml-2">• Click a row for details</span>
              </p>
            </div>
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

          {validatorsError ? (
            <div className="p-12 text-center">
              <p className="text-red-500 font-semibold">Error loading validators:</p>
              <pre className="text-red-400 text-sm mt-2 whitespace-pre-wrap">{String(validatorsError)}</pre>
            </div>
          ) : loadingValidators ? (
            <div className="p-12 text-center">
              <p className="text-gray-500">Loading validators...</p>
            </div>
          ) : filteredValidators && filteredValidators.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider min-w-[350px]">Node ID</th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Status</th>
                    <th className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider">Weight</th>
                    <th
                      className="px-6 py-3 text-right text-xs font-semibold text-gray-700 uppercase tracking-wider cursor-pointer hover:bg-gray-100 transition-colors select-none"
                      onClick={handleSortToggle}
                    >
                      <div className="flex items-center justify-end gap-1">
                        {subnetDetails?.subnet_type === 'primary' || subnetDetails?.subnet_type === 'regular' ? 'Stake' : 'Balance'}
                        {sortOrder === 'desc' && <ArrowDown size={14} />}
                        {sortOrder === 'asc' && <ArrowUp size={14} />}
                        {!sortOrder && <ArrowUpDown size={14} className="text-gray-400" />}
                      </div>
                    </th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Registered</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {filteredValidators.map((validator) => (
                    <tr
                      key={validator.node_id + validator.validation_id}
                      className={`hover:bg-blue-50 transition-colors cursor-pointer ${!validator.active ? 'opacity-60' : ''}`}
                      onClick={() => navigate(`/p-chain/subnet/${subnetId}/validator/${encodeURIComponent(validator.node_id)}`)}
                    >
                      <td className="px-6 py-4 whitespace-nowrap min-w-[350px]">
                        <div className="flex flex-col gap-1">
                          <div className="flex items-center gap-2">
                            <span className={`text-sm font-medium font-mono ${validator.active ? 'text-gray-900' : 'text-gray-500'}`}>
                              {validator.node_id || <span className="text-gray-400 italic">Unknown</span>}
                            </span>
                            {validator.node_id && (
                              <button
                                onClick={(e) => { e.stopPropagation(); copyToClipboard(validator.node_id); }}
                                className="text-gray-400 hover:text-gray-600 transition-colors"
                                title="Copy Node ID"
                              >
                                <Copy size={12} />
                              </button>
                            )}
                            <ChevronRight size={14} className="text-gray-300" />
                          </div>
                          {validator.validation_id && (
                            <div className="flex items-center gap-2">
                              <span className="text-xs text-gray-500 font-mono" title="Validation ID">
                                {validator.validation_id.substring(0, 12)}...
                              </span>
                              <button
                                onClick={(e) => { e.stopPropagation(); copyToClipboard(validator.validation_id); }}
                                className="text-gray-400 hover:text-gray-600 transition-colors"
                                title="Copy Validation ID"
                              >
                                <Copy size={12} />
                              </button>
                            </div>
                          )}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="flex items-center gap-2">
                          {validator.active ? (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-100 text-green-800">
                              <CheckCircle size={12} /> Active
                            </span>
                          ) : (
                            <span className="inline-flex items-center gap-1.5 px-2.5 py-0.5 rounded-full text-xs font-medium bg-red-100 text-red-800">
                              <XCircle size={12} /> Inactive
                            </span>
                          )}
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Activity size={16} className="text-gray-400" />
                          <span className={`text-sm font-semibold ${validator.active ? 'text-gray-900' : 'text-gray-500'}`}>{formatWeight(validator.weight)}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap text-right">
                        <div className="flex items-center justify-end gap-2">
                          <Wallet size={16} className="text-gray-400" />
                          <span className={`text-sm ${validator.active ? 'text-gray-900' : 'text-gray-500'}`}>{formatBalance(validator.balance)}</span>
                        </div>
                      </td>
                      <td className="px-6 py-4 whitespace-nowrap">
                        <div className="text-xs text-gray-500 space-y-1">
                          {validator.created_time && (
                            <p>{new Date(validator.created_time).toLocaleDateString()}</p>
                          )}
                          {validator.created_tx_type && (
                            <p className="text-gray-400">{validator.created_tx_type}</p>
                          )}
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

          {/* Show All / Show Less Button */}
          {totalValidators > 10 && filteredValidators && filteredValidators.length > 0 && (
            <div className="px-6 py-4 border-t border-gray-200 text-center">
              <button
                onClick={() => setShowAll(!showAll)}
                className="text-blue-600 hover:text-blue-800 text-sm font-medium transition-colors"
              >
                {showAll ? `Show Less` : `Show All ${totalValidators} Validators`}
              </button>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default SubnetValidators;
