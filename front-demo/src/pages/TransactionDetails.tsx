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
  CheckCircle,
  Copy,
  FileText
} from 'lucide-react';

interface TransactionDetails {
  tx_id: string;
  tx_type: string;
  block_number: number;
  formatted_time: string;
  tx_data: any;
}

function TransactionDetailsPage() {
  const { txId } = useParams<{ txId: string }>();
  const { url } = useClickhouseUrl();

  const clickhouse = useMemo(() => createClient({
    url,
    username: "anonymous",
  }), [url]);

  const pChainId = 0;

  // Transaction Details Query
  const { data: txDetails, isLoading: loadingTx, error: txError } = useQuery<TransactionDetails>({
    queryKey: ['transaction-details', txId, url],
    queryFn: async () => {
      const result = await clickhouse.query({
        query: `
          SELECT
            tx_id,
            tx_type,
            block_number,
            formatDateTime(block_time, '%Y-%m-%dT%H:%i:%sZ') as formatted_time,
            tx_data
          FROM p_chain_txs
          WHERE p_chain_id = ${pChainId}
            AND tx_id = '${txId}'
          LIMIT 1
        `,
        format: 'JSONEachRow',
      });
      const data = await result.json<TransactionDetails>();
      console.log('Transaction query result:', data);
      return (data as TransactionDetails[])[0];
    },
  });

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
  };

  const formatTimestamp = (timestamp: string) => {
    const date = new Date(timestamp);
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

  // Helper function to format field names to be more readable
  const formatFieldName = (fieldName: string): string => {
    // Convert camelCase or PascalCase to Title Case with spaces
    return fieldName
      .replace(/([A-Z])/g, ' $1')
      .replace(/^./, (str) => str.toUpperCase())
      .trim();
  };

  // Helper function to check if a value is a primitive that should be displayed directly
  const isPrimitiveValue = (value: any): boolean => {
    return typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean';
  };

  // Helper function to format AVAX amounts (if the value looks like nanoAVAX)
  const formatIfAmount = (key: string, value: any): string => {
    const amountFields = ['balance', 'weight', 'wght', 'amt', 'amount', 'stake', 'delegationfee'];
    if (typeof value === 'string' && amountFields.some(field => key.toLowerCase().includes(field))) {
      const num = parseInt(value);
      if (!isNaN(num) && num > 1000000) {
        return `${(num / 1e9).toLocaleString()} AVAX`;
      }
    }
    return value;
  };

  // Helper function to format timestamps
  const formatIfTimestamp = (key: string, value: any): string => {
    const timeFields = ['start', 'end', 'time', 'timestamp'];
    if (typeof value === 'string' && timeFields.some(field => key.toLowerCase().includes(field))) {
      const num = parseInt(value);
      if (!isNaN(num) && num > 1000000000 && num < 9999999999999) {
        // Unix timestamp (either seconds or milliseconds)
        const timestamp = num > 9999999999 ? num : num * 1000;
        return new Date(timestamp).toLocaleString();
      }
    }
    return value;
  };

  if (loadingTx) {
    return (
      <div className="p-8 flex items-center justify-center min-h-[400px]">
        <p className="text-gray-500">Loading transaction details...</p>
      </div>
    );
  }

  if (txError) {
    return (
      <div className="p-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900">Error Loading Transaction</h2>
        <p className="text-red-600 mt-2">{String(txError)}</p>
        <p className="text-gray-600 mt-2">Transaction ID: {txId}</p>
        <Link to="/p-chain/overview" className="text-blue-600 hover:text-blue-800 mt-4 inline-block">
          ← Back to Overview
        </Link>
      </div>
    );
  }

  if (!txDetails) {
    return (
      <div className="p-8 text-center">
        <h2 className="text-2xl font-bold text-gray-900">Transaction Not Found</h2>
        <p className="text-gray-600 mt-2">Transaction ID: {txId}</p>
        <p className="text-sm text-gray-500 mt-2">This transaction does not exist in the database.</p>
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
            <div className="flex items-start justify-between mb-6">
              <div className="flex-1">
                <h1 className="text-2xl font-bold text-gray-900 mb-2">Transaction Details</h1>
                <div className="flex items-center gap-2">
                  <span className="inline-flex items-center gap-1.5 px-3 py-1 rounded-full text-sm font-medium bg-green-100 text-green-800">
                    <CheckCircle size={16} /> SUCCESS
                  </span>
                  <span className="inline-flex items-center px-3 py-1 rounded-full text-sm font-medium bg-blue-100 text-blue-800">
                    {txDetails.tx_type}
                  </span>
                </div>
              </div>
            </div>

            <div className="space-y-4">
              {/* Transaction Hash */}
              <div className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg">
                <Hash size={18} className="text-gray-400 mt-1" />
                <div className="flex-1 min-w-0">
                  <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">Hash</p>
                  <div className="flex items-center gap-2">
                    <p className="text-sm font-mono text-gray-900 break-all">{txDetails.tx_id}</p>
                    <button
                      onClick={() => copyToClipboard(txDetails.tx_id)}
                      className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                      title="Copy Hash"
                    >
                      <Copy size={14} />
                    </button>
                  </div>
                </div>
              </div>

              {/* Main Info Grid */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full bg-blue-500 flex items-center justify-center flex-shrink-0">
                    <span className="text-white font-bold text-sm">P</span>
                  </div>
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide">Chain</p>
                    <p className="text-sm font-medium text-gray-900">P-CHAIN</p>
                  </div>
                </div>

                <div className="flex items-center gap-3">
                  <Hash size={18} className="text-gray-400" />
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide">Block</p>
                    <Link
                      to={`/p-chain/block/${txDetails.block_number}`}
                      className="text-sm font-medium text-blue-600 hover:text-blue-800"
                    >
                      #{txDetails.block_number.toLocaleString()}
                    </Link>
                  </div>
                </div>

                <div className="flex items-center gap-3">
                  <Clock size={18} className="text-gray-400" />
                  <div>
                    <p className="text-xs text-gray-500 uppercase tracking-wide">Timestamp</p>
                    <p className="text-sm font-medium text-gray-900">
                      {new Date(txDetails.formatted_time).toLocaleString()}
                    </p>
                    <p className="text-xs text-gray-500">
                      ({formatTimestamp(txDetails.formatted_time)})
                    </p>
                  </div>
                </div>
              </div>

              {/* Transaction-Specific Details */}
              {txDetails.tx_data && Object.keys(txDetails.tx_data).length > 0 && (
                <div className="border-t border-gray-200 pt-4 space-y-4">
                  <h3 className="text-sm font-semibold text-gray-700 uppercase tracking-wide">Transaction Data</h3>

                  {/* Render all primitive fields dynamically */}
                  {(() => {
                    const primitiveFields: { key: string; value: any }[] = [];
                    const objectFields: { key: string; value: any }[] = [];
                    const arrayFields: { key: string; value: any[] }[] = [];

                    // Categorize fields
                    Object.entries(txDetails.tx_data).forEach(([key, value]) => {
                      if (value === null || value === undefined) return;

                      if (isPrimitiveValue(value)) {
                        primitiveFields.push({ key, value });
                      } else if (Array.isArray(value)) {
                        arrayFields.push({ key, value });
                      } else if (typeof value === 'object') {
                        objectFields.push({ key, value });
                      }
                    });

                    return (
                      <>
                        {/* Primitive Fields Grid */}
                        {primitiveFields.length > 0 && (
                          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                            {primitiveFields.map(({ key, value }) => {
                              const displayValue = formatIfTimestamp(key, formatIfAmount(key, value));
                              const isLongString = typeof displayValue === 'string' && displayValue.length > 30;
                              const isSubnetId = key === 'SubnetID' && value !== '11111111111111111111111111111111LpoYY';

                              return (
                                <div key={key} className="p-3 bg-gray-50 rounded-lg">
                                  <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">
                                    {formatFieldName(key)}
                                  </p>
                                  <div className="flex items-center gap-2">
                                    {isSubnetId ? (
                                      <Link
                                        to={`/p-chain/subnet/${value}`}
                                        className={`text-sm ${isLongString ? 'font-mono' : 'font-medium'} text-blue-600 hover:text-blue-800 break-all`}
                                      >
                                        {displayValue}
                                      </Link>
                                    ) : (
                                      <p className={`text-sm ${isLongString ? 'font-mono' : 'font-medium'} text-gray-900 break-all`}>
                                        {displayValue}
                                      </p>
                                    )}
                                    {typeof value === 'string' && value.length > 20 && (
                                      <button
                                        onClick={() => copyToClipboard(value)}
                                        className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                                        title={`Copy ${formatFieldName(key)}`}
                                      >
                                        <Copy size={14} />
                                      </button>
                                    )}
                                  </div>
                                </div>
                              );
                            })}
                          </div>
                        )}

                        {/* Object Fields */}
                        {objectFields.map(({ key, value }) => (
                          <div key={key} className="border-t border-gray-200 pt-4">
                            <h4 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-3">
                              {formatFieldName(key)}
                            </h4>
                            <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                              {Object.entries(value).map(([subKey, subValue]) => {
                                if (subValue === null || subValue === undefined) return null;
                                if (typeof subValue === 'object' && !Array.isArray(subValue)) return null;

                                const displayValue = formatIfTimestamp(subKey, formatIfAmount(subKey, subValue));
                                const isLongString = typeof displayValue === 'string' && displayValue.length > 30;

                                return (
                                  <div key={subKey} className="p-3 bg-gray-50 rounded-lg">
                                    <p className="text-xs text-gray-500 uppercase tracking-wide mb-1">
                                      {formatFieldName(subKey)}
                                    </p>
                                    <div className="flex items-center gap-2">
                                      <p className={`text-sm ${isLongString ? 'font-mono' : 'font-medium'} text-gray-900 break-all`}>
                                        {displayValue}
                                      </p>
                                      {typeof subValue === 'string' && subValue.length > 20 && (
                                        <button
                                          onClick={() => copyToClipboard(subValue)}
                                          className="text-gray-400 hover:text-gray-600 transition-colors flex-shrink-0"
                                          title={`Copy ${formatFieldName(subKey)}`}
                                        >
                                          <Copy size={14} />
                                        </button>
                                      )}
                                    </div>
                                  </div>
                                );
                              })}
                            </div>
                          </div>
                        ))}

                        {/* Array Fields */}
                        {arrayFields.map(({ key, value }) => (
                          <div key={key} className="border-t border-gray-200 pt-4">
                            <h4 className="text-sm font-semibold text-gray-700 uppercase tracking-wide mb-3">
                              {formatFieldName(key)} ({value.length})
                            </h4>
                            <div className="space-y-2">
                              {value.map((item: any, idx: number) => (
                                <div key={idx} className="p-3 bg-gray-50 rounded-lg border border-gray-200">
                                  <div className="flex items-center justify-between mb-2">
                                    <span className="text-xs font-semibold text-gray-500">
                                      {formatFieldName(key).replace(/s$/, '')} #{idx}
                                    </span>
                                  </div>
                                  {typeof item === 'object' && item !== null ? (
                                    <div className="space-y-1 text-xs">
                                      {Object.entries(item).map(([itemKey, itemValue]: [string, any]) => {
                                        if (itemValue === null || itemValue === undefined) return null;

                                        // Handle nested objects
                                        if (typeof itemValue === 'object' && !Array.isArray(itemValue)) {
                                          return (
                                            <div key={itemKey} className="mt-2">
                                              <span className="text-gray-500 font-semibold">{formatFieldName(itemKey)}:</span>
                                              <div className="ml-4 mt-1 space-y-1">
                                                {Object.entries(itemValue).map(([nestedKey, nestedValue]: [string, any]) => {
                                                  if (nestedValue === null || nestedValue === undefined) return null;
                                                  if (typeof nestedValue === 'object') return null;

                                                  const displayValue = formatIfTimestamp(nestedKey, formatIfAmount(nestedKey, nestedValue));
                                                  return (
                                                    <div key={nestedKey} className="flex gap-2">
                                                      <span className="text-gray-500 min-w-[100px]">{formatFieldName(nestedKey)}:</span>
                                                      <span className="font-mono text-gray-700 break-all">{displayValue}</span>
                                                    </div>
                                                  );
                                                })}
                                              </div>
                                            </div>
                                          );
                                        }

                                        // Handle arrays within items
                                        if (Array.isArray(itemValue)) {
                                          return (
                                            <div key={itemKey} className="mt-2">
                                              <span className="text-gray-500 font-semibold">{formatFieldName(itemKey)}:</span>
                                              <div className="ml-4 mt-1">
                                                {itemValue.map((arrItem: any, arrIdx: number) => (
                                                  <div key={arrIdx} className="font-mono text-gray-700 break-all">
                                                    {typeof arrItem === 'string' ? arrItem : JSON.stringify(arrItem)}
                                                  </div>
                                                ))}
                                              </div>
                                            </div>
                                          );
                                        }

                                        const displayValue = formatIfTimestamp(itemKey, formatIfAmount(itemKey, itemValue));
                                        return (
                                          <div key={itemKey} className="flex gap-2">
                                            <span className="text-gray-500 min-w-[100px]">{formatFieldName(itemKey)}:</span>
                                            <span className="font-mono text-gray-700 break-all">{displayValue}</span>
                                          </div>
                                        );
                                      })}
                                    </div>
                                  ) : (
                                    <div className="text-xs text-gray-700">{String(item)}</div>
                                  )}
                                </div>
                              ))}
                            </div>
                          </div>
                        ))}
                      </>
                    );
                  })()}

                  {/* Raw JSON Data - Collapsible */}
                  <details className="border-t border-gray-200 pt-4">
                    <summary className="text-xs text-gray-500 uppercase tracking-wide mb-2 cursor-pointer hover:text-gray-700">
                      Raw Transaction Data (Click to expand)
                    </summary>
                    <pre className="text-xs font-mono text-gray-700 bg-gray-100 p-3 rounded overflow-x-auto mt-2">
                      {JSON.stringify(txDetails.tx_data, null, 2)}
                    </pre>
                  </details>
                </div>
              )}
            </div>
          </div>
        </div>
      </div>
    </PageTransition>
  );
}

export default TransactionDetailsPage;
