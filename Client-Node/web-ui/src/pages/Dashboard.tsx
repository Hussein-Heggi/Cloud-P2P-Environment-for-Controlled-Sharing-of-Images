import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatusBar from '../components/StatusBar';
import {
  getDos,
  getNotifications,
  getRequests,
  approveRequest,
  denyRequest,
  requestView,
  DosClient,
  PendingViewNotification,
  PendingRequest,
} from '../api/client';

export default function Dashboard() {
  const [activeTab, setActiveTab] = useState<'owner' | 'viewer'>('viewer');

  // Viewer state
  const [dosClients, setDosClients] = useState<DosClient[]>([]);
  const [myRequests, setMyRequests] = useState<PendingRequest[]>([]);
  const [selectedImage, setSelectedImage] = useState<{ owner: string; image: string } | null>(null);
  const [requestedViews, setRequestedViews] = useState(5);

  // Owner state
  const [notifications, setNotifications] = useState<PendingViewNotification[]>([]);

  // Fetch DOS for viewer tab
  useEffect(() => {
    if (activeTab === 'viewer') {
      const fetchDos = async () => {
        try {
          const data = await getDos();
          console.log('[UI DEBUG] Fetched DOS data:', data);
          console.log('[UI DEBUG] Number of users in response:', data.users.length);
          console.log('[UI DEBUG] User names:', data.users.map(u => u.name));
          setDosClients(data.users);
          console.log('[UI DEBUG] State updated with', data.users.length, 'clients');
        } catch (error) {
          console.error('Failed to fetch DOS:', error);
        }
      };
      fetchDos();
      const interval = setInterval(fetchDos, 3000);
      return () => clearInterval(interval);
    }
  }, [activeTab]);

  // Fetch viewer requests
  useEffect(() => {
    if (activeTab === 'viewer') {
      const fetchRequests = async () => {
        try {
          const data = await getRequests();
          setMyRequests(data);
        } catch (error) {
          console.error('Failed to fetch requests:', error);
        }
      };
      fetchRequests();
      const interval = setInterval(fetchRequests, 2000);
      return () => clearInterval(interval);
    }
  }, [activeTab]);

  // Fetch owner notifications
  useEffect(() => {
    if (activeTab === 'owner') {
      const fetchNotifications = async () => {
        try {
          const data = await getNotifications();
          setNotifications(data);
        } catch (error) {
          console.error('Failed to fetch notifications:', error);
        }
      };
      fetchNotifications();
      const interval = setInterval(fetchNotifications, 2000);
      return () => clearInterval(interval);
    }
  }, [activeTab]);

  const handleRequestView = async () => {
    if (!selectedImage) return;

    try {
      await requestView({
        owner: selectedImage.owner,
        image_name: selectedImage.image,
        requested_views: requestedViews,
      });
      setSelectedImage(null);
      alert(`Request sent for ${selectedImage.image} from ${selectedImage.owner}`);
    } catch (error) {
      console.error('Failed to request view:', error);
      alert('Failed to send request');
    }
  };

  const handleApprove = async (requestId: number) => {
    try {
      await approveRequest(requestId);
      setNotifications(notifications.filter(n => n.request_id !== requestId));
      alert(`Request ${requestId} approved`);
    } catch (error) {
      console.error('Failed to approve:', error);
      alert('Failed to approve request');
    }
  };

  const handleDeny = async (requestId: number) => {
    try {
      await denyRequest(requestId);
      setNotifications(notifications.filter(n => n.request_id !== requestId));
      alert(`Request ${requestId} denied`);
    } catch (error) {
      console.error('Failed to deny:', error);
      alert('Failed to deny request');
    }
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <StatusBar />

      <div className="container mx-auto px-4 py-6">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-3xl font-bold text-gray-800">Dashboard</h1>
          <div className="flex space-x-3">
            <Link to="/downloads" className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-md">
              View Downloads
            </Link>
            <Link to="/permissions" className="bg-purple-500 hover:bg-purple-600 text-white px-4 py-2 rounded-md">
              Permissions
            </Link>
          </div>
        </div>

        {/* Tabs */}
        <div className="mb-6">
          <div className="border-b border-gray-300">
            <div className="flex space-x-4">
              <button
                className={`px-4 py-2 font-medium ${
                  activeTab === 'viewer'
                    ? 'border-b-2 border-blue-500 text-blue-600'
                    : 'text-gray-600 hover:text-gray-800'
                }`}
                onClick={() => setActiveTab('viewer')}
              >
                Viewer Mode
              </button>
              <button
                className={`px-4 py-2 font-medium ${
                  activeTab === 'owner'
                    ? 'border-b-2 border-blue-500 text-blue-600'
                    : 'text-gray-600 hover:text-gray-800'
                }`}
                onClick={() => setActiveTab('owner')}
              >
                Owner Mode
              </button>
            </div>
          </div>
        </div>

        {/* Viewer Tab */}
        {activeTab === 'viewer' && (
          <div className="space-y-6">
            {/* DOS Browser */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">Directory of Services (DOS)</h2>
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        User
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Status
                      </th>
                      <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                        Available Images
                      </th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {(() => {
                      console.log('[UI DEBUG] Rendering DOS table with', dosClients.length, 'clients');
                      console.log('[UI DEBUG] Clients to render:', dosClients.map(c => ({ name: c.name, images: c.images.length })));
                      return dosClients.map((client) => {
                        const isOnline = client.online ?? true;
                        console.log('[UI DEBUG] Rendering client row:', client.name);
                        return (
                          <tr key={client.name}>
                            <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                              {client.name}
                            </td>
                            <td className="px-6 py-4 whitespace-nowrap text-sm">
                              <span className={`inline-flex items-center px-2 py-1 rounded-full text-xs font-medium ${
                                isOnline ? 'bg-green-100 text-green-800' : 'bg-gray-200 text-gray-600'
                              }`}>
                                {isOnline ? 'Online' : 'Offline'}
                              </span>
                            </td>
                            <td className="px-6 py-4 text-sm text-gray-500">
                              {client.images.length > 0 ? (
                                <div className="space-y-1">
                                  {client.images.map((img) => (
                                    <button
                                      key={img}
                                      onClick={() => setSelectedImage({ owner: client.name, image: img })}
                                      className="block text-blue-600 hover:text-blue-800 hover:underline"
                                    >
                                      {img}
                                    </button>
                                  ))}
                                </div>
                              ) : (
                                <span className="text-gray-400">No images</span>
                              )}
                            </td>
                          </tr>
                        );
                      });
                    })()}
                  </tbody>
                </table>
              </div>
            </div>

            {/* My Requests */}
            <div className="bg-white rounded-lg shadow p-6">
              <h2 className="text-xl font-semibold mb-4">My Requests</h2>
              {myRequests.length === 0 ? (
                <p className="text-gray-500">No pending requests</p>
              ) : (
                <div className="space-y-2">
                  {myRequests.map((req) => (
                    <div key={req.request_id} className="border border-gray-200 rounded p-4 flex justify-between items-center">
                      <div>
                        <div className="font-medium">{req.image} from {req.owner}</div>
                        <div className="text-sm text-gray-500">Request ID: {req.request_id}</div>
                      </div>
                      <span className={`px-3 py-1 rounded-full text-sm ${
                        req.status === 'approved' ? 'bg-green-100 text-green-800' :
                        req.status === 'rejected' ? 'bg-red-100 text-red-800' :
                        'bg-yellow-100 text-yellow-800'
                      }`}>
                        {req.status}
                      </span>
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )}

        {/* Owner Tab */}
        {activeTab === 'owner' && (
          <div className="bg-white rounded-lg shadow p-6">
            <h2 className="text-xl font-semibold mb-4">Pending View Requests</h2>
            {notifications.length === 0 ? (
              <p className="text-gray-500">No pending requests</p>
            ) : (
              <div className="space-y-4">
                {notifications.map((notif) => (
                  <div key={notif.request_id} className="border border-gray-200 rounded p-4">
                    <div className="flex justify-between items-start">
                      <div>
                        <div className="font-medium text-lg">{notif.viewer} wants to view "{notif.image}"</div>
                        <div className="text-sm text-gray-600 mt-1">
                          Requested views: {notif.requested_views}
                        </div>
                        <div className="text-xs text-gray-400 mt-1">
                          Request ID: {notif.request_id} • {new Date(notif.timestamp * 1000).toLocaleString()}
                        </div>
                      </div>
                      <div className="flex space-x-2">
                        <button
                          onClick={() => handleApprove(notif.request_id)}
                          className="bg-green-500 hover:bg-green-600 text-white px-4 py-2 rounded-md"
                        >
                          Approve
                        </button>
                        <button
                          onClick={() => handleDeny(notif.request_id)}
                          className="bg-red-500 hover:bg-red-600 text-white px-4 py-2 rounded-md"
                        >
                          Deny
                        </button>
                      </div>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
        )}
      </div>

      {/* Request Modal */}
      {selectedImage && (
        <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center p-4">
          <div className="bg-white rounded-lg p-6 max-w-md w-full">
            <h3 className="text-lg font-semibold mb-4">Request Image</h3>
            <p className="mb-4">
              <strong>Owner:</strong> {selectedImage.owner}<br />
              <strong>Image:</strong> {selectedImage.image}
            </p>
            <div className="mb-4">
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Number of views requested:
              </label>
              <input
                type="number"
                min="1"
                max="100"
                value={requestedViews}
                onChange={(e) => setRequestedViews(parseInt(e.target.value) || 1)}
                className="w-full border border-gray-300 rounded-md px-3 py-2"
              />
            </div>
            <div className="flex space-x-2">
              <button
                onClick={handleRequestView}
                className="flex-1 bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-md"
              >
                Send Request
              </button>
              <button
                onClick={() => setSelectedImage(null)}
                className="flex-1 bg-gray-300 hover:bg-gray-400 text-gray-800 px-4 py-2 rounded-md"
              >
                Cancel
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
