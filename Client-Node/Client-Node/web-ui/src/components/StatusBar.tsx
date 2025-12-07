import { useQuery } from '@tanstack/react-query'
import { getStatus } from '../api/client'

export default function StatusBar() {
  const { data: status } = useQuery({
    queryKey: ['status'],
    queryFn: getStatus,
    refetchInterval: 5000,
  })

  return (
    <div className="bg-gray-800 border-b border-gray-700 px-6 py-3">
      <div className="flex items-center justify-between">
        <div className="flex items-center space-x-6">
          <div className="flex items-center space-x-2">
            <div className={`w-2 h-2 rounded-full ${status?.connected ? 'bg-green-500' : 'bg-red-500'}`} />
            <span className="text-sm text-gray-300">
              {status?.connected ? 'Connected' : 'Disconnected'}
            </span>
          </div>
          {status && (
            <>
              <div className="text-sm text-gray-300">
                <span className="text-gray-400">User:</span> {status.username}
              </div>
              <div className="text-sm text-gray-300">
                <span className="text-gray-400">Server:</span> {status.server_addr}
              </div>
              <div className="text-sm text-gray-300">
                <span className="text-gray-400">DOS v</span>{status.dos_version}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  )
}
