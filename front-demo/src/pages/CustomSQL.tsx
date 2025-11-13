import { useState, useEffect } from 'react';
import PageTransition from '../components/PageTransition';
import QueryEditor from '../components/QueryEditor';

const STORAGE_KEY = 'customSqlQuery';

const EXAMPLE_QUERY = `-- USDC Transfer events by week
-- address: 0xb97ef9ef8734c71904d8002f8b6bc66dd9c48a6e (USDC on Avalanche C-Chain)
-- topic0: Transfer(address,address,uint256) event signature
SELECT 
  toStartOfWeek(block_time) as week,
  count(*) as transfer_count,
  uniq(tx_from) as unique_senders,
  -- Decode transfer amount from data field (already binary, UInt256 big-endian)
  formatReadableQuantity(round(sum(reinterpretAsUInt256(reverse(data)) / 1000000.0), 2)) as total_amount_usdc
FROM raw_logs
WHERE chain_id = 43114
  AND address = unhex('b97ef9ef8734c71904d8002f8b6bc66dd9c48a6e')
  AND topic0 = unhex('ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef')
  AND block_time >= '2025-06-01'
GROUP BY week
ORDER BY week DESC`;

function CustomSQL() {
  const [query, setQuery] = useState(() => {
    const saved = localStorage.getItem(STORAGE_KEY);
    return saved || EXAMPLE_QUERY;
  });

  useEffect(() => {
    localStorage.setItem(STORAGE_KEY, query);
  }, [query]);

  return (
    <PageTransition>
      <div className="p-8 space-y-6">
        <h1 className="text-3xl font-bold text-gray-900">Custom SQL</h1>
        <QueryEditor
          initialQuery={query}
          onQueryChange={setQuery}
        />
      </div>
    </PageTransition>
  );
}

export default CustomSQL;

