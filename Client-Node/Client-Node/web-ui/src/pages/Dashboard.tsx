import { useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import StatusBar from '../components/StatusBar'
import {
  getDos,
  getNotifications,
  getMyRequests,
  approveRequest,
  denyRequest,
  requestView,
} from '../api/client'

export default function Dashboard() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const [activeTab, setActiveTab] = useState<'owner' | 'viewer'>('viewer')

  // Queries
  const { data: dos } = useQuery({
    queryKey: ['dos'],
    queryFn: getDos,
    refetchInterval: 5000,
  })

  const { data: notifications } = useQuery({
    queryKey: ['notifications'],
    queryFn: getNotifications,
    refetchInterval: 2000,
  })

  const { data: myRequests } = useQuery({
    queryKey: ['myRequests'],
    queryFn: getMyRequests,
    refetchInterval: 2000,
  })

  // Mutations
  const approveMutation = useMutation({
    mutationFn: approveRequest,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })

  const denyMutation = useMutation({
    mutationFn: denyRequest,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['notifications'] })
    },
  })

  const requestViewMutation = useMutation({
    mutationFn: requestView,
    onSuccess: () => {
      queryClient.invalidateQueries({ queryKey: ['myRequests'] })
      setShowRequestModal(false)
    },
  })

  // Request modal state
  const [showRequestModal, setShowRequestModal] = useState(false)
  const [selectedOwner, setSelectedOwner] = useState('')
  const [selectedImage, setSelectedImage] = useState('')
  const [requestedViews, setRequestedViews] = useState(5)

  const handleRequestImage = (owner: string, image: string) => {
    setSelectedOwner(owner)
    setSelectedImage(image)
    setShowRequestModal(true)
  }

  const submitRequest = () => {
    requestViewMutation.mutate({
      owner: selectedOwner,
      image_name: selectedImage,
      requested_views: requestedViews,
    })
  }

  return (
    <div className="min-h-screen bg-gray-900">
      <StatusBar />

      <div className="p-6">
        <div className="flex items-center justify-between mb-6">
          <h1 className="text-3xl font-bold text-white">Dashboard</h1>
          <button
            onClick={() => navigate('/downloads')}
            className="px-4 py-2 bg-blue-600 hover:bg-blue-700 text-white rounded-lg transition"
          >
            View Downloads
          </button>
        </div>

        {/* Tabs */}
        <div className="flex space-x-2 mb-6">
          <button
            onClick={() => setActiveTab('viewer')}
            className={`px-6 py-3 rounded-lg font-semibold transition ${
              activeTab === 'viewer'
                ? 'bg-blue-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
            }`}
          >
            Viewer Mode
          </button>
          <button
            onClick={() => setActiveTab('owner')}
            className={`px-6 py-3 rounded-lg font-semibold transition ${
              activeTab === 'owner'
                ? 'bg-purple-600 text-white'
                : 'bg-gray-800 text-gray-400 hover:bg-gray-700'
            }`}
          >
            Owner Mode
          </button>
        </div>

        {/* Viewer Tab */}
        {activeTab === 'viewer' && (
          <div className="space-y-6">
            {/* DOS Browser */}
            <div className="bg-gray-800 rounded-lg p-6">
              <h2 className="text-xl font-semibold text-white mb-4">Available Images (DOS-C)</h2>
              <div className="overflow-x-auto">
                <table className="w-full">
                  <thead className="bg-gray-700">
                    <tr>
                      <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">User</th>
                      <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Status</th>
                      <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Images</th>
                      <th className="px-4 py-3 text-left text-sm font-semibold text-gray-200">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-700">
                    {dos?.users.map((user) => (
                      <tr key={user.name} className="hover:bg-gray-700/50">
                        <td className="px-4 py-3 text-gray-300">{user.name}</td>
                        <td className="px-4 py-3">
                          <span
                            className={`px-2 py-1 text-xs rounded-full ${
                              user.online ? 'bg-green-500/20 text-green-400' : 'bg-gray-500/20 text-gray-400'
                            }`}
                          >
                            {user.online ? 'Online' : 'Offline'}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-gray-300">
                          {user.images.length === 0 ? (
                            <span className="text-gray-500">No images</span>
                          ) : (
                            <div className="flex flex-wrap gap-2">
                              {user.images.map((img) => (
                                <button
                                  key={img}
                                  onClick={() => handleRequestImage(user.name, img)}
                                  className="px-3 py-1 bg-blue-500/20 hover:bg-blue-500/30 text-blue-300 rounded-lg text-sm transition"
                                >
                                  {img}
                                </button>
                              ))}
                            </div>
                          )}
                        </td>
                        <td className="px-4 py-3 text-gray-400 text-sm">{user.ip}:{user.port}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </div>

            {/* My Requests */}
            <div className="bg-gray-800 rounded-lg p-6">
              <h2 className="text-xl font-semibold text-white mb-4">My Requests</h2>
              {myRequests && myRequests.length > 0 ? (
                <div className="space-y-2">
                  {myRequests.map((req) => (
                    <div
                      key={req.request_id}
                      className="flex items-center justify-between bg-gray-700 rounded-lg p-4"
                    >
                      <div>
                        <div className="text-white font-medium">
                          {req.owner} / {req.image}
                        </div>
                        <div className="text-sm text-gray-400">
                          Request ID: {req.request_id} • {new Date(req.timestamp * 1000).toLocaleString()}
                        </div>
                      </div>
                      <span
                        className={`px-3 py-1 rounded-full text-sm ${
                          req.status === 'pending'
                            ? 'bg-yellow-500/20 text-yellow-400'
                            : req.status === 'approved'
                            ? 'bg-green-500/20 text-green-400'
                            : 'bg-red-500/20 text-red-400'
                        }`}
                      >
                        {req.status}
                      </span>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-gray-400 text-center py-8">No pending requests</p>
              )}
            </div>
          </div>
        )}

        {/* Owner Tab */}
        {activeTab === 'owner' && (
          <div className="space-y-6">
            {/* Pending Approvals */}
            <div className="bg-gray-800 rounded-lg p-6">
              <h2 className="text-xl font-semibold text-white mb-4">Pending View Requests</h2>
              {notifications && notifications.length > 0 ? (
                <div className="space-y-2">
                  {notifications.map((notif) => (
                    <div
                      key={notif.request_id}
                      className="flex items-center justify-between bg-gray-700 rounded-lg p-4"
                    >
                      <div>
                        <div className="text-white font-medium">
                          {notif.viewer} requests {notif.image}
                        </div>
                        <div className="text-sm text-gray-400">
                          Views requested: {notif.requested_views} • Request ID: {notif.request_id}
                        </div>
                        <div className="text-xs text-gray-500">
                          {new Date(notif.timestamp * 1000).toLocaleString()}
                        </div>
                      </div>
                      <div className="flex space-x-2">
                        <button
                          onClick={() => approveMutation.mutate(notif.request_id)}
                          className="px-4 py-2 bg-green-600 hover:bg-green-700 text-white rounded-lg transition"
                          disabled={approveMutation.isPending}
                        >
                          Approve
                        </button>
                        <button
                          onClick={() => denyMutation.mutate(notif.request_id)}
                          className="px-4 py-2 bg-red-600 hover:bg-red-700 text-white rounded-lg transition"
                          disabled={denyMutation.isPending}
                        >
                          Deny
                        </button>
                      </div>
                    </div>
                  ))}
                </div>
              ) : (
                <p className="text-gray-400 text-center py-8">No pending requests</p>
              )}
            </div>
          </div>
        )}
      </div>

      {/* Request Modal */}
      {showRequestModal && (
        <div className="fixed inset-0 bg-black/50 flex items-center justify-center p-4 z-50">
          <div className="bg-gray-800 rounded-lg p-6 max-w-md w-full">
            <h2 className="text-2xl font-bold text-white mb-4">Request Image</h2>
            <div className="space-y-4">
              <div>
                <label className="block text-sm text-gray-400 mb-1">Owner</label>
                <div className="text-white">{selectedOwner}</div>
              </div>
              <div>
                <label className="block text-sm text-gray-400 mb-1">Image</label>
                <div className="text-white">{selectedImage}</div>
              </div>
              <div>
                <label className="block text-sm text-gray-400 mb-2">Number of Views</label>
                <input
                  type="number"
                  value={requestedViews}
                  onChange={(e) => setRequestedViews(parseInt(e.target.value) || 1)}
                  min="1"
                  max="100"
                  className="w-full px-4 py-2 bg-gray-700 border border-gray-600 rounded-lg text-white focus:outline-none focus:ring-2 focus:ring-blue-500"
                />
              </div>
              <div className="flex space-x-2 pt-4">
                <button
                  onClick={submitRequest}
                  disabled={requestViewMutation.isPending}
                  className="flex-1 px-4 py-2 bg-blue-600 hover:bg-blue-700 disabled:bg-gray-600 text-white rounded-lg transition"
                >
                  {requestViewMutation.isPending ? 'Sending...' : 'Send Request'}
                </button>
                <button
                  onClick={() => setShowRequestModal(false)}
                  className="flex-1 px-4 py-2 bg-gray-700 hover:bg-gray-600 text-white rounded-lg transition"
                >
                  Cancel
                </button>
              </div>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
