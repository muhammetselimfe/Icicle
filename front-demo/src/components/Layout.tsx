import { NavLink, Outlet, useLocation } from 'react-router-dom';
import { BarChart3, FileCode, RefreshCw } from 'lucide-react';
import { motion } from 'framer-motion';
import { useClickhouseUrl } from '../hooks/useClickhouseUrl';

function Layout() {
  const location = useLocation();
  const { url, setUrl } = useClickhouseUrl();
  const menuItems = [
    { path: '/evm-metrics/43114/7d', pathPrefix: '/evm-metrics', label: 'EVM Metrics', icon: BarChart3 },
    { path: '/custom-sql', pathPrefix: '/custom-sql', label: 'Custom SQL', icon: FileCode },
    { path: '/sync-status', pathPrefix: '/sync-status', label: 'Sync status', icon: RefreshCw },
  ];

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-50 to-gray-100">
      {/* Floating top nav */}
      <div className="fixed top-6 left-1/2 -translate-x-1/2 z-50">
        <nav className="bg-white/70 backdrop-blur-xl rounded-full shadow-lg shadow-black/5 border border-gray-200/50 px-2 py-2">
          <ul className="flex items-center gap-1 relative">
            {menuItems.map((item) => {
              const isActive = location.pathname.startsWith(item.pathPrefix);
              return (
                <li key={item.path} className="relative">
                  {isActive && (
                    <motion.div
                      layoutId="active-pill"
                      className="absolute inset-0 bg-gray-900 rounded-full shadow-md"
                      transition={{ type: 'spring', stiffness: 400, damping: 30 }}
                    />
                  )}
                  <NavLink
                    to={item.path}
                    className={`relative flex items-center gap-2 px-5 py-2.5 rounded-full text-sm font-medium transition-colors z-10 ${isActive
                      ? 'text-white'
                      : 'text-gray-700 hover:text-gray-900'
                      }`}
                  >
                    <item.icon size={18} strokeWidth={2} />
                    {item.label}
                  </NavLink>
                </li>
              );
            })}

            {/* Server Selector */}
            <li className="ml-2 pl-2 border-l border-gray-300">
              <select
                value={url}
                onChange={(e) => {
                  setUrl(e.target.value);
                  // Refresh page to ensure clean state with new server
                  window.location.reload();
                }}
                className="px-3 py-2 text-sm font-medium text-gray-700 bg-transparent border-none rounded-full hover:bg-gray-100/50 cursor-pointer focus:outline-none focus:ring-2 focus:ring-gray-900/20"
              >
                <option value="https://node01-8123.containerman.me/">PoC Server</option>
                <option value="http://localhost:8123">localhost</option>
              </select>
            </li>
          </ul>
        </nav>
      </div>

      {/* Main content */}
      <div className="pt-24 max-w-7xl mx-auto">
        <Outlet />
      </div>
    </div>
  );
}

export default Layout;

