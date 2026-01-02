import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import { useMemo } from 'react';
import { useParams, Link } from 'react-router-dom';
import PageTransition from '../components/PageTransition';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import {
  ArrowLeft,
  Clock,
  Hash,
  FileText,
  Copy
} from 'lucide-react';

interface BlockDetails {
  block_number: number;
  block_time: string;
  tx_count: number;
  block_hash?: string;
  parent_id?: string;
  proposer_id?: string;
  proposer_node_id?: string;
  block_type?: string;
}

interface Transaction {
  tx_id: string;
  tx_type: string;
  formatted_time: string;
}

function BlockDetailsPage() {
  const { blockNumber } = useParams<{ blockNumber: string }>();
  const { url } = useClickhouseUrl();

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  // P-Chain ID from config.yaml
  const pChainId = 0;

  // Block Details Query
  const { data: blockDetails, isLoading: loadingBlock, error: blockError } = useQuery<BlockDetails>({
    queryKey: ['block-details', blockNumber, url],
    queryFn: async () => {
      const blockNum = parseInt(blockNumber || '0', 10);
      console.log('Querying for block number:', blockNum);
      const result = await clickhouse.query({
        query: `
          SELECT
            block_number,
            count(*) as tx_count,
            formatDateTime(max(block_time), '%Y-%m-%dT%H:%i:%sZ') as block_time,
            any(tx_id) as block_hash,
            any(tx_data.ParentID) as parent_id,
            any(tx_data.ProposerID) as proposer_id,
            any(tx_data.NodeID) as proposer_node_id,
            any(tx_type) as block_type
          FROM p_chain_txs
          WHERE p_chain_id = ${pChainId}
            AND block_number = toUInt64(${blockNum})
          GROUP BY block_number
          LIMIT 1
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<BlockDetails>();
      console.log('Block query result:', data);
      return (data as BlockDetails[])[0];
    },
  });

  // Transactions Query
  const { data: transactions, isLoading: loadingTxs, error: txError } = useQuery<Transaction[]>({
    queryKey: ['block-transactions', blockNumber, url],
    queryFn: async () => {
      const blockNum = parseInt(blockNumber || '0', 10);
      console.log('Querying transactions for block:', blockNum);
      const result = await clickhouse.query({
        query: `
          SELECT
            tx_id,
            tx_type,
            formatDateTime(block_time, '%Y-%m-%dT%H:%i:%sZ') as formatted_time
          FROM p_chain_txs
          WHERE p_chain_id = ${pChainId}
            AND block_number = toUInt64(${blockNum})
          ORDER BY block_time ASC
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<Transaction>();
      console.log('Transactions query result:', data);
      console.log('Transaction count:', Array.isArray(data) ? data.length : 0);
      return data as Transaction[];
    },
    enabled: !!blockDetails, // Only run after block details are loaded
  });

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const formatTimestamp = (timestamp: string) => {
    // Parse as UTC by appending 'Z' if not present
    const utcTimestamp = timestamp.includes('Z') ? timestamp : timestamp + ' UTC';
    const date = new Date(utcTimestamp);
    const now = new Date();
    const diffMs = now.getTime() - date.getTime();
    const diffSecs = Math.floor(diffMs / 1000);
    const diffMins = Math.floor(diffSecs / 60);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffSecs < 60) return `${diffSecs} seconds ago`;
    if (diffMins < 60) return `${diffMins} minutes ago`;
    if (diffHours < 24) return `${diffHours} hours ago`;
    return `${diffDays} days ago`;
  };

  if (loadingBlock) {
    return (
      <div className="p-8 flex items-center justify-center min-h-[400px]">
        <p className="text-gray-500">Loading block details...</p>
      </div>
    );
  }

  if (blockError) {
    return (
      <div className="p-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900">Error Loading Block</h2>
        <p className="text-red-600 mt-2">{String(blockError)}</p>
        <p className="text-gray-600 mt-2">Block Number: {blockNumber}</p>
        <Link to="/p-chain/overview" className="text-blue-600 hover:text-blue-800 mt-4 inline-block">
          ← Back to Overview
        </Link>
      </div>
    );
  }

  if (!blockDetails) {
    return (
      <div className="p-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900">Block Not Found</h2>
        <p className="text-gray-600 mt-2">Block Number: {blockNumber}</p>
        <p className="text-sm text-gray-500 mt-2">This block may not exist in the database yet, or the block number may be incorrect.</p>
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
                <h1 className="text-2xl font-bold text-gray-900 flex items-center gap-3">
                  <Hash size={28} className="text-gray-400" />
                  Block #{blockDetails.block_number.toLocaleString()}
                </h1>
                <div className="mt-4 space-y-4">
                  {/* Block Hash */}
                  {blockDetails.block_hash && (
                    <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg">
                      <Hash size={18} className="text-gray-400 mt-1" />
                      <div className="flex-1 min-w-0">
                        <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Hash</p>
                        <div className="flex items-center gap-2">
                          <p className="text-sm font-mono text-gray-900 break-all">{blockDetails.block_hash}</p>
                          <button
                            onClick={() => copyToClipboard(blockDetails.block_hash!)}
                            className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                            title="Copy Hash"
                          >
                            <Copy size={14} />
                          </button>
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Main Info Grid */}
                  <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                    <div className="flex items-center gap-3">
                      <Clock size={18} className="text-gray-400" />
                      <div>
                        <p className="text-xs text-gray-500 uppercase tracking-wide">Timestamp</p>
                        <p className="text-sm font-medium text-gray-900">
                          {new Date(blockDetails.block_time).toLocaleString()}
                        </p>
                        <p className="text-xs text-gray-500">
                          ({formatTimestamp(blockDetails.block_time)})
                        </p>
                      </div>
                    </div>
                    <div className="flex items-center gap-3">
                      <FileText size={18} className="text-gray-400" />
                      <div>
                        <p className="text-xs text-gray-500 uppercase tracking-wide">Transactions</p>
                        <p className="text-sm font-medium text-gray-900">
                          {blockDetails.tx_count} transaction{blockDetails.tx_count !== 1 ? 's' : ''}
                        </p>
                      </div>
                    </div>
                    {blockDetails.block_type && (
                      <div className="flex items-center gap-3">
                        <FileText size={18} className="text-gray-400" />
                        <div>
                          <p className="text-xs text-gray-500 uppercase tracking-wide">Type</p>
                          <p className="text-sm font-medium text-gray-900">{blockDetails.block_type}</p>
                        </div>
                      </div>
                    )}
                  </div>

                  {/* Proposer Information */}
                  {(blockDetails.proposer_id || blockDetails.proposer_node_id || blockDetails.parent_id) && (
                    <div className="border-t border-gray-200 pt-4 space-y-3">
                      <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Block Details</h3>

                      {blockDetails.parent_id && (
                        <div className="flex items-start gap-3">
                          <div className="flex-1 min-w-0">
                            <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Parent Block ID</p>
                            <div className="flex items-center gap-2">
                              <p className="text-sm font-mono text-gray-700 break-all">{blockDetails.parent_id}</p>
                              <button
                                onClick={() => copyToClipboard(blockDetails.parent_id!)}
                                className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                                title="Copy Parent ID"
                              >
                                <Copy size={14} />
                              </button>
                            </div>
                          </div>
                        </div>
                      )}

                      {blockDetails.proposer_id && (
                        <div className="flex items-start gap-3">
                          <div className="flex-1 min-w-0">
                            <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Proposer ID</p>
                            <div className="flex items-center gap-2">
                              <p className="text-sm font-mono text-gray-700 break-all">{blockDetails.proposer_id}</p>
                              <button
                                onClick={() => copyToClipboard(blockDetails.proposer_id!)}
                                className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                                title="Copy Proposer ID"
                              >
                                <Copy size={14} />
                              </button>
                            </div>
                          </div>
                        </div>
                      )}

                      {blockDetails.proposer_node_id && (
                        <div className="flex items-start gap-3">
                          <div className="flex-1 min-w-0">
                            <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Proposer Node ID</p>
                            <div className="flex items-center gap-2">
                              <p className="text-sm font-mono text-gray-700 break-all">{blockDetails.proposer_node_id}</p>
                              <button
                                onClick={() => copyToClipboard(blockDetails.proposer_node_id!)}
                                className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                                title="Copy Proposer Node ID"
                              >
                                <Copy size={14} />
                              </button>
                            </div>
                          </div>
                        </div>
                      )}
                    </div>
                  )}
                </div>
              </div>
            </div>
          </div>
        </div>

        {/* Transactions List */}
        <div className="bg-white rounded-lg shadow overflow-hidden">
          <div className="px-6 py-4 border-b border-gray-200">
            <h2 className="text-lg font-bold text-gray-900">Transactions</h2>
            <p className="text-sm text-gray-600 mt-1">
              {blockDetails.tx_count} transaction{blockDetails.tx_count !== 1 ? 's' : ''} in this block
            </p>
          </div>

          {loadingTxs ? (
            <div className="p-12 text-center">
              <p className="text-gray-500">Loading transactions...</p>
            </div>
          ) : txError ? (
            <div className="p-12 text-center">
              <p className="text-red-600">Error loading transactions: {String(txError)}</p>
            </div>
          ) : transactions && transactions.length > 0 ? (
            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Transaction ID</th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Type</th>
                    <th className="px-6 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider">Time</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {transactions.map((tx) => (
                    <tr key={tx.tx_id} className="hover:bg-gray-50 transition-colors">
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2">
                          <span className="text-sm font-mono text-gray-900" title={tx.tx_id}>
                            {tx.tx_id.substring(0, 16)}...
                          </span>
                          <button
                            onClick={() => copyToClipboard(tx.tx_id)}
                            className="text-gray-400 hover:text-gray-600 transition-colors"
                            title="Copy Transaction ID"
                          >
                            <Copy size={14} />
                          </button>
                        </div>
                      </td>
                      <td className="px-6 py-4">
                        <span className="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-blue-100 text-blue-800">
                          {tx.tx_type}
                        </span>
                      </td>
                      <td className="px-6 py-4">
                        <span className="text-sm text-gray-600">
                          {new Date(tx.formatted_time).toLocaleTimeString()}
                        </span>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : (
            <div className="p-12 text-center">
              <p className="text-gray-500">No transactions found in this block.</p>
              <p className="text-xs text-gray-400 mt-2">Check the browser console for more details.</p>
            </div>
          )}
        </div>
      </div>
    </PageTransition>
  );
}

export default BlockDetailsPage;
