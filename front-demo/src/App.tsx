import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import Layout from './components/Layout';
import Metrics from './pages/Metrics';
import CustomSQL from './pages/CustomSQL';
import SyncStatus from './pages/SyncStatus';
import IndexerDemo from './pages/IndexerDemo';
import PChainOverview from './pages/PChainOverview';
import SubnetValidators from './pages/SubnetValidators';
import ValidatorDetails from './pages/ValidatorDetails';
import BlockDetails from './pages/BlockDetails';
import TransactionDetails from './pages/TransactionDetails';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      refetchOnWindowFocus: false,
      retry: 1,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route path="/" element={<Layout />}>
            <Route index element={<Navigate to="/evm-metrics/43114/7d" replace />} />
            <Route path="evm-metrics" element={<Navigate to="/evm-metrics/43114/7d" replace />} />
            <Route path="evm-metrics/:chainId/:timePeriod" element={<Metrics />} />
            <Route path="p-chain/overview" element={<PChainOverview />} />
            <Route path="p-chain/subnet/:subnetId" element={<SubnetValidators />} />
            <Route path="p-chain/subnet/:subnetId/validator/:nodeId" element={<ValidatorDetails />} />
            <Route path="p-chain/block/:blockNumber" element={<BlockDetails />} />
            <Route path="p-chain/tx/:txId" element={<TransactionDetails />} />
            <Route path="custom-sql" element={<CustomSQL />} />
            <Route path="indexer-demo" element={<IndexerDemo />} />
            <Route path="sync-status" element={<SyncStatus />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App
