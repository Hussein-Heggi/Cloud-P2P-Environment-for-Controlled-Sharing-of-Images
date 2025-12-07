import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import StatusBar from '../components/StatusBar'
import { getDownloads, extractImage } from '../api/client'

export default function Downloads() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const { data: downloads } = useQuery({
    queryKey: ['downloads'],
    queryFn: getDownloads,
    refetchInterval: 3000,
  })

  const extractMutation = useMutation({
    mutationFn: extractImage,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['downloads'] })
    },
  })

  const handleExtract = (embeddedPath: string) => {
    const filename = embeddedPath.split('/').pop() || embeddedPath
    extractMutation.mutate(filename)
  }

  return (
    <div className="min-h-screen bg-gray-900">
      <StatusBar />

      <div className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-3xl font-bold text-white">Downloads</h1>
          <button
            onClick={() => navigate('/dashboard')}
            className="px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition"
          >
            Back to Dashboard
          </button>
        </div>

        {downloads && downloads.length > 0 ? (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {downloads.map((download, idx) => (
              <div key={idx} className="bg-gray-800 rounded-lg p-6 space-y-4">
                <div className="border-b border-gray-700 pb-4">
                  <h3 className="text-xl font-semibold text-white">{download.image_name}</h3>
                  <p className="text-sm text-gray-400">Owner: {download.owner}</p>
                </div>

                {/* Embedded Image */}
                <div>
                  <h4 className="text-sm font-medium text-gray-300 mb-2">Embedded Image</h4>
                  <img
                    src={`http://localhost:3001/${download.embedded_path}`}
                    alt={`${download.image_name} embedded`}
                    className="w-full rounded-lg border border-gray-700"
                    onError={(e) => {
                      e.currentTarget.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="100" height="100"%3E%3Crect width="100" height="100" fill="%23333"/%3E%3Ctext x="50" y="50" text-anchor="middle" fill="%23999"%3ENo image%3C/text%3E%3C/svg%3E'
                    }}
                  />
                  <p className="text-xs text-gray-500 mt-1 break-all">{download.embedded_path}</p>
                </div>

                {/* Extract Button */}
                {!download.extracted_path && (
                  <button
                    onClick={() => handleExtract(download.embedded_path)}
                    disabled={extractMutation.isPending}
                    className="w-full px-4 py-2 bg-purple-600 hover:bg-purple-700 disabled:bg-gray-600 text-white rounded-lg transition"
                  >
                    {extractMutation.isPending ? 'Extracting...' : 'Extract True Image'}
                  </button>
                )}

                {/* Extracted Image */}
                {download.extracted_path && (
                  <div>
                    <h4 className="text-sm font-medium text-green-400 mb-2">True Image (Extracted)</h4>
                    <img
                      src={`http://localhost:3001/${download.extracted_path}`}
                      alt={`${download.image_name} extracted`}
                      className="w-full rounded-lg border-2 border-green-500"
                      onError={(e) => {
                        e.currentTarget.src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="100" height="100"%3E%3Crect width="100" height="100" fill="%23333"/%3E%3Ctext x="50" y="50" text-anchor="middle" fill="%23999"%3ENo image%3C/text%3E%3C/svg%3E'
                      }}
                    />
                    <p className="text-xs text-gray-500 mt-1 break-all">{download.extracted_path}</p>
                  </div>
                )}

                {/* Metadata */}
                {download.metadata && (
                  <div>
                    <h4 className="text-sm font-medium text-gray-300 mb-2">Metadata</h4>
                    <pre className="text-xs text-gray-400 bg-gray-900 p-3 rounded border border-gray-700 overflow-auto max-h-40">
                      {JSON.stringify(JSON.parse(download.metadata), null, 2)}
                    </pre>
                  </div>
                )}
              </div>
            ))}
          </div>
        ) : (
          <div className="bg-gray-800 rounded-lg p-12 text-center">
            <p className="text-gray-400 text-lg">No downloads yet</p>
            <p className="text-gray-500 text-sm mt-2">Request images from other users to download them</p>
          </div>
        )}
      </div>
    </div>
  )
}
