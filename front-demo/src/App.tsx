import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import Layout from './components/Layout';
import Metrics from './pages/Metrics';
import CustomSQL from './pages/CustomSQL';
import SyncStatus from './pages/SyncStatus';

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
            <Route index element={<Navigate to="/metrics/43114/hour" replace />} />
            <Route path="metrics" element={<Navigate to="/metrics/43114/hour" replace />} />
            <Route path="metrics/:chainId/:granularity" element={<Metrics />} />
            <Route path="custom-sql" element={<CustomSQL />} />
            <Route path="sync-status" element={<SyncStatus />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}

export default App
