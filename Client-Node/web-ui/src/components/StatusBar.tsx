import { useEffect, useState } from 'react';
import { getStatus, StatusResponse } from '../api/client';

export default function StatusBar() {
  const [status, setStatus] = useState<StatusResponse | null>(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const fetchStatus = async () => {
      try {
        const data = await getStatus();
        setStatus(data);
        setLoading(false);
      } catch (error) {
        console.error('Failed to fetch status:', error);
        setLoading(false);
      }
    };

    // Fetch initially
    fetchStatus();

    // Refresh every 5 seconds
    const interval = setInterval(fetchStatus, 5000);

    return () => clearInterval(interval);
  }, []);

  if (loading) {
    return (
      <div className="bg-gray-800 text-white px-4 py-2 flex items-center justify-between">
        <div className="text-sm">Loading...</div>
      </div>
    );
  }

  return (
    <div className="bg-gray-800 text-white px-4 py-2 flex items-center justify-between">
      <div className="flex items-center space-x-6">
        <div className="flex items-center space-x-2">
          <div className={`w-2 h-2 rounded-full ${status?.connected ? 'bg-green-400' : 'bg-red-400'}`}></div>
          <span className="text-sm font-medium">
            {status?.connected ? 'Connected' : 'Disconnected'}
          </span>
        </div>
        {status?.username && (
          <div className="text-sm">
            <span className="text-gray-400">User:</span> <span className="font-medium">{status.username}</span>
          </div>
        )}
      </div>
      <div className="text-xs text-gray-400">
        Cloud-P2P Client v0.1.0
      </div>
    </div>
  );
}
