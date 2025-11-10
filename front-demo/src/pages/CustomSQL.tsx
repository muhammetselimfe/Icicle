import { useState } from 'react';
import { useMutation } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import PageTransition from '../components/PageTransition';
import AiHelpButton from '../components/AiHelpButton';
import { Play, Loader2, ChevronLeft, ChevronRight, ChevronsLeft, ChevronsRight } from 'lucide-react';

const clickhouse = createClient({
  url: 'http://localhost:8123',
  username: "anonymous_heavy",
});

const PAGE_SIZE = 200;

interface QueryResult {
  columns: string[];
  rows: Record<string, any>[];
  executionTime: number;
  dbExecutionTime?: number;
  rowsRead?: number;
  bytesRead?: number;
  rowsPerSec?: number;
  bytesPerSec?: number;
}

function CustomSQL() {
  const [query, setQuery] = useState('SELECT count(distinct(from)) FROM raw_traces where chain_id = 43114');
  const [currentPage, setCurrentPage] = useState(1);
  const [result, setResult] = useState<QueryResult | null>(null);

  const { mutate: executeQuery, isPending, error } = useMutation({
    mutationFn: async (sqlQuery: string) => {
      const startTime = performance.now();
      const queryResult = await clickhouse.query({
        query: sqlQuery,
        format: 'JSONEachRow',
      });
      const data = await queryResult.json();
      const endTime = performance.now();

      const rows = (Array.isArray(data) ? data : []) as Record<string, any>[];
      const columns = rows.length > 0 ? Object.keys(rows[0]) : [];

      // Extract statistics from X-ClickHouse-Summary response header
      const summaryHeader = queryResult.response_headers['x-clickhouse-summary'];
      let summary = null;
      if (summaryHeader && typeof summaryHeader === 'string') {
        try {
          summary = JSON.parse(summaryHeader);
        } catch (e) {
          console.error('Failed to parse ClickHouse summary:', e);
        }
      }

      const elapsedNs = summary?.elapsed_ns ? parseInt(summary.elapsed_ns) : undefined;
      const readRows = summary?.read_rows ? parseInt(summary.read_rows) : undefined;
      const readBytes = summary?.read_bytes ? parseInt(summary.read_bytes) : undefined;

      // Calculate throughput metrics
      const elapsedSec = elapsedNs ? elapsedNs / 1_000_000_000 : undefined;
      const rowsPerSec = elapsedSec && readRows ? readRows / elapsedSec : undefined;
      const bytesPerSec = elapsedSec && readBytes ? readBytes / elapsedSec : undefined;

      return {
        columns,
        rows,
        executionTime: endTime - startTime,
        dbExecutionTime: elapsedNs ? elapsedNs / 1_000_000 : undefined,
        rowsRead: readRows,
        bytesRead: readBytes,
        rowsPerSec,
        bytesPerSec,
      };
    },
    onSuccess: (data) => {
      setResult(data);
      setCurrentPage(1);
    },
  });

  const handleExecute = () => {
    if (query.trim()) {
      executeQuery(query.trim());
    }
  };

  const totalPages = result ? Math.ceil(result.rows.length / PAGE_SIZE) : 0;
  const startRow = (currentPage - 1) * PAGE_SIZE;
  const endRow = Math.min(startRow + PAGE_SIZE, result?.rows.length || 0);
  const currentPageRows = result?.rows.slice(startRow, endRow) || [];

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') {
      e.preventDefault();
      handleExecute();
    }
  };

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        <h1 className="text-3xl font-bold text-gray-900">Custom SQL</h1>

        {/* Query Editor */}
        <div className="bg-white rounded-lg shadow p-6 space-y-4">
          <div className="space-y-2">
            <label htmlFor="query" className="block text-sm font-medium text-gray-700">
              SQL Query
            </label>
            <textarea
              id="query"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              onKeyDown={handleKeyDown}
              className="w-full h-32 px-4 py-3 font-mono text-sm border border-gray-300 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-blue-500 resize-y"
              placeholder="Enter your SQL query here..."
            />
            <p className="text-xs text-gray-500">
              Press Ctrl+Enter (or Cmd+Enter) to execute
            </p>
          </div>

          <div className="flex items-center gap-3">
            <button
              onClick={handleExecute}
              disabled={isPending || !query.trim()}
              className="flex items-center gap-2 px-6 py-2.5 bg-blue-600 text-white rounded-lg font-medium hover:bg-blue-700 disabled:bg-gray-400 disabled:cursor-not-allowed cursor-pointer transition-colors"
            >
              {isPending ? (
                <>
                  <Loader2 size={18} className="animate-spin" />
                  Executing...
                </>
              ) : (
                <>
                  <Play size={18} />
                  Execute Query
                </>
              )}
            </button>

            <AiHelpButton />

            {/* Query Stats */}
            {result && result.dbExecutionTime !== undefined && (
              <div className="flex items-center gap-6 text-sm text-gray-600">
                <div className="flex items-center gap-2">
                  <span>⚡</span>
                  <span>Elapsed:</span>
                  <span className="font-semibold text-gray-900">
                    {(result.dbExecutionTime / 1000).toFixed(3)}s
                  </span>
                </div>

                {result.rowsRead !== undefined && result.rowsPerSec !== undefined && (
                  <div className="flex items-center gap-2">
                    <span>📊</span>
                    <span>Rows:</span>
                    <span className="font-semibold text-gray-900">
                      {result.rowsRead >= 1_000_000
                        ? `${(result.rowsRead / 1_000_000).toFixed(1)}M`
                        : result.rowsRead >= 1_000
                          ? `${(result.rowsRead / 1_000).toFixed(1)}K`
                          : result.rowsRead.toString()}
                    </span>
                    <span className="text-gray-400">@</span>
                    <span className="text-gray-500 font-medium">
                      {result.rowsPerSec >= 1_000_000_000
                        ? `${(result.rowsPerSec / 1_000_000_000).toFixed(1)}B/s`
                        : result.rowsPerSec >= 1_000_000
                          ? `${(result.rowsPerSec / 1_000_000).toFixed(0)}M/s`
                          : `${(result.rowsPerSec / 1_000).toFixed(0)}K/s`}
                    </span>
                  </div>
                )}

                {result.bytesRead !== undefined && result.bytesPerSec !== undefined && (
                  <div className="flex items-center gap-2">
                    <span>💾</span>
                    <span>Data:</span>
                    <span className="font-semibold text-gray-900">
                      {result.bytesRead >= 1024 * 1024 * 1024
                        ? `${(result.bytesRead / 1024 / 1024 / 1024).toFixed(1)}GB`
                        : result.bytesRead >= 1024 * 1024
                          ? `${(result.bytesRead / 1024 / 1024).toFixed(0)}MB`
                          : `${(result.bytesRead / 1024).toFixed(0)}KB`}
                    </span>
                    <span className="text-gray-400">@</span>
                    <span className="text-gray-500 font-medium">
                      {result.bytesPerSec >= 1024 * 1024 * 1024
                        ? `${(result.bytesPerSec / 1024 / 1024 / 1024).toFixed(1)}GB/s`
                        : result.bytesPerSec >= 1024 * 1024
                          ? `${(result.bytesPerSec / 1024 / 1024).toFixed(0)}MB/s`
                          : `${(result.bytesPerSec / 1024).toFixed(0)}KB/s`}
                    </span>
                  </div>
                )}
              </div>
            )}
          </div>
        </div>

        {/* Error Display */}
        {error && (
          <div className="bg-red-50 border border-red-200 rounded-lg p-4">
            <h3 className="text-sm font-semibold text-red-900 mb-1">Query Error</h3>
            <p className="text-sm text-red-700 font-mono">{error.message}</p>
          </div>
        )}

        {/* Results Table */}
        {result && result.rows.length > 0 && (
          <div className="bg-white rounded-lg shadow overflow-hidden">
            <div className="p-4 border-b border-gray-200">
              <h3 className="text-lg font-semibold text-gray-900">Results</h3>
              <p className="text-sm text-gray-600 mt-1">
                Showing {startRow + 1}-{endRow} of {result.rows.length} rows
              </p>
            </div>

            <div className="overflow-x-auto">
              <table className="w-full">
                <thead className="bg-gray-50 border-b border-gray-200">
                  <tr>
                    {result.columns.map((column) => (
                      <th
                        key={column}
                        className="px-4 py-3 text-left text-xs font-semibold text-gray-700 uppercase tracking-wider whitespace-nowrap"
                      >
                        {column}
                      </th>
                    ))}
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-200">
                  {currentPageRows.map((row, rowIndex) => (
                    <tr
                      key={rowIndex}
                      className={rowIndex % 2 === 0 ? 'bg-white' : 'bg-gray-50'}
                    >
                      {result.columns.map((column) => (
                        <td
                          key={column}
                          className="px-4 py-3 text-sm text-gray-900 whitespace-nowrap"
                        >
                          {row[column] !== null && row[column] !== undefined
                            ? String(row[column])
                            : <span className="text-gray-400 italic">null</span>}
                        </td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>

            {/* Pagination Controls */}
            {totalPages > 1 && (
              <div className="px-4 py-3 border-t border-gray-200 flex items-center justify-between">
                <div className="text-sm text-gray-600">
                  Page {currentPage} of {totalPages}
                </div>
                <div className="flex items-center gap-2">
                  <button
                    onClick={() => setCurrentPage(1)}
                    disabled={currentPage === 1}
                    className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer transition-colors"
                    title="First page"
                  >
                    <ChevronsLeft size={18} />
                  </button>
                  <button
                    onClick={() => setCurrentPage(currentPage - 1)}
                    disabled={currentPage === 1}
                    className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer transition-colors"
                    title="Previous page"
                  >
                    <ChevronLeft size={18} />
                  </button>
                  <button
                    onClick={() => setCurrentPage(currentPage + 1)}
                    disabled={currentPage === totalPages}
                    className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer transition-colors"
                    title="Next page"
                  >
                    <ChevronRight size={18} />
                  </button>
                  <button
                    onClick={() => setCurrentPage(totalPages)}
                    disabled={currentPage === totalPages}
                    className="p-2 border border-gray-300 rounded-lg hover:bg-gray-50 disabled:opacity-50 disabled:cursor-not-allowed cursor-pointer transition-colors"
                    title="Last page"
                  >
                    <ChevronsRight size={18} />
                  </button>
                </div>
              </div>
            )}
          </div>
        )}

        {/* Empty State */}
        {result && result.rows.length === 0 && (
          <div className="bg-white rounded-lg shadow p-8 text-center">
            <p className="text-gray-600">Query executed successfully but returned no rows.</p>
          </div>
        )}
      </div>
    </PageTransition>
  );
}

export default CustomSQL;

