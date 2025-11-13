import { useState, useEffect, useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { createClient } from '@clickhouse/client-web';
import PageTransition from '../components/PageTransition';
import QueryEditor from '../components/QueryEditor';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';
import { useSyncStatus } from '../hooks/useSyncStatus';

interface Chain {
    chain_id: number;
    name: string;
}

interface SampleData {
    wallet: string;
    token: string;
    block_number: number;
}

interface PopularToken {
    token: string;
}

function IndexerDemo() {
    const [selectedChainId, setSelectedChainId] = useState<number>(43114);
    const [sampleData, setSampleData] = useState<SampleData | null>(null);
    const [popularToken, setPopularToken] = useState<PopularToken | null>(null);
    const [loadingSample, setLoadingSample] = useState(false);
    const { url } = useClickhouseUrl();
    const { chains: syncStatusChains } = useSyncStatus();

    const clickhouse = useMemo(() => createClient({
        url,
        username: "anonymous",
    }), [url]);

    const { data: chains, isLoading: loadingChains } = useQuery<Chain[]>({
        queryKey: ['chains', url],
        queryFn: async () => {
            const result = await clickhouse.query({
                query: 'SELECT chain_id, name FROM chain_status FINAL WHERE chain_id != 0',
                format: 'JSONEachRow',
            });
            const data = await result.json<Chain>();
            return data as Chain[];
        },
        staleTime: 5 * 60 * 1000,
    });

    useEffect(() => {
        let cancelled = false;

        async function fetchSampleData() {
            setLoadingSample(true);
            setSampleData(null);
            setPopularToken(null);

            try {
                // Fetch sample wallet-token pair for balance query
                const sampleResult = await clickhouse.query({
                    query: `
            SELECT 
              hex(wallet) as wallet,
              hex(token) as token,
              block_number
            FROM erc20_balances
            WHERE chain_id = ${selectedChainId}
              AND balance > 0
            ORDER BY block_number DESC
            LIMIT 1
          `,
                    format: 'JSONEachRow',
                });
                const sampleDataRaw = await sampleResult.json<SampleData>();

                // Fetch most popular token for top holders query
                const popularResult = await clickhouse.query({
                    query: `
            SELECT hex(token) as token
            FROM erc20_balances
            WHERE chain_id = ${selectedChainId}
              AND balance > 0
            GROUP BY token
            ORDER BY count() DESC
            LIMIT 1
          `,
                    format: 'JSONEachRow',
                });
                const popularTokenRaw = await popularResult.json<PopularToken>();

                if (!cancelled) {
                    if (sampleDataRaw && Array.isArray(sampleDataRaw) && sampleDataRaw.length > 0) {
                        setSampleData(sampleDataRaw[0]);
                    }
                    if (popularTokenRaw && Array.isArray(popularTokenRaw) && popularTokenRaw.length > 0) {
                        setPopularToken(popularTokenRaw[0]);
                    }
                }
            } catch (err) {
                console.error('Error fetching sample data:', err);
            } finally {
                setLoadingSample(false);
            }
        }

        fetchSampleData();

        return () => {
            cancelled = true;
        };
    }, [selectedChainId, clickhouse]);

    const balanceAtBlockQuery = sampleData
        ? `-- Get wallet balance at a specific block
-- This query retrieves the token balance for a wallet at or before a block
-- Chain ID: ${selectedChainId}
-- Token: 0x${sampleData.token}
-- Wallet: 0x${sampleData.wallet}
SELECT 
  balance as balance_wei
FROM erc20_balances
WHERE chain_id = ${selectedChainId}
  AND wallet = unhex('${sampleData.wallet}')
  AND token = unhex('${sampleData.token}')
  AND block_number <= ${sampleData.block_number - 123}
ORDER BY block_number DESC
LIMIT 1`
        : '-- Loading sample data...';

    const topHoldersQuery = popularToken
        ? `-- Top 5 holders of the most popular token on this chain
-- Token: 0x${popularToken.token}
-- Chain ID: ${selectedChainId}
SELECT 
  hex(wallet) as wallet_address,
  MAX(balance) / 1000000.0 as balance_formatted,
  MAX(block_number) as latest_block
FROM erc20_balances
WHERE chain_id = ${selectedChainId}
  AND token = unhex('${popularToken.token}')
  AND balance > 0
GROUP BY wallet
ORDER BY balance_formatted DESC
LIMIT 5`
        : '-- Loading sample data...';

    return (
        <PageTransition>
            <div className="p-8 space-y-6">
                <div>
                    <h1 className="text-3xl font-bold text-gray-900">Indexer Demo</h1>
                    <p className="text-gray-600 mt-2">
                        Explore ERC20 token balances indexed from blockchain data. Select a chain to see live examples.
                    </p>
                </div>

                {/* Chain Selector */}
                <div className="bg-white rounded-lg shadow p-4">
                    <div className="flex items-center gap-3">
                        <label className="text-sm font-semibold text-gray-700 whitespace-nowrap">Chain:</label>
                        {loadingChains ? (
                            <p className="text-sm text-gray-500">Loading chains...</p>
                        ) : chains ? (
                            <select
                                value={selectedChainId}
                                onChange={(e) => setSelectedChainId(parseInt(e.target.value))}
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
                        ) : (
                            <p className="text-sm text-red-600">Error loading chains</p>
                        )}
                        {loadingSample && (
                            <span className="text-sm text-gray-500 ml-2">Loading sample data...</span>
                        )}
                    </div>
                </div>

                {sampleData && popularToken && !loadingSample && (
                    <>
                        {/* Query 1: Balance at Block */}
                        <div className="border-t border-gray-200 pt-8">
                            <QueryEditor
                                key={`balance-${selectedChainId}-${sampleData.wallet}-${sampleData.token}`}
                                initialQuery={balanceAtBlockQuery}
                                title="Query 1: Balance at Block"
                                description="Get the token balance of a specific wallet at a given block. This demonstrates time-travel queries for historical balance lookups."
                            />
                        </div>

                        {/* Query 2: Top Token Holders */}
                        <div className="border-t border-gray-200 pt-8">
                            <QueryEditor
                                key={`holders-${selectedChainId}-${popularToken.token}`}
                                initialQuery={topHoldersQuery}
                                title="Query 2: Top Token Holders"
                                description="Find the wallets with the highest balances for the most popular token on this chain. Useful for identifying major holders and token distribution."
                            />
                        </div>
                    </>
                )}

                {(!sampleData || !popularToken) && !loadingSample && (
                    <div className="bg-white rounded-lg shadow p-8 text-center">
                        <p className="text-gray-600">No balance data available for this chain.</p>
                    </div>
                )}
            </div>
        </PageTransition>
    );
}

export default IndexerDemo;

