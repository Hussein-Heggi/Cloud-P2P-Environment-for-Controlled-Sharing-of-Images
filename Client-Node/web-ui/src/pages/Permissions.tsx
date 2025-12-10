import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatusBar from '../components/StatusBar';
import {
  getLocalAccessMap,
  getIncomingAdjustRequests,
  ownerAdjustViews,
  ownerRevokeAccess,
  approveAdjustRequest,
  rejectAdjustRequest,
  LocalAccessGrant,
  IncomingAdjustRequest
} from '../api/client';

export default function Permissions() {
  const [grants, setGrants] = useState<LocalAccessGrant[]>([]);
  const [incomingRequests, setIncomingRequests] = useState<IncomingAdjustRequest[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchData();

    // Auto-refresh every 2 seconds
    const interval = setInterval(fetchData, 2000);
    return () => clearInterval(interval);
  }, []);

  const fetchData = async () => {
    try {
      const [grantsData, requestsData] = await Promise.all([
        getLocalAccessMap(),
        getIncomingAdjustRequests(),
      ]);
      setGrants(grantsData.grants);
      setIncomingRequests(requestsData.requests);
      setLoading(false);
    } catch (error) {
      console.error('Failed to fetch data:', error);
      setLoading(false);
    }
  };

  const handleAdjust = async (viewer: string, image: string, current: number) => {
    const newCount = prompt(`Current: ${current} views\nEnter new view count (must be > 0):`);
    if (newCount === null) return; // User cancelled

    const newViews = parseInt(newCount);
    if (isNaN(newViews) || newViews <= 0) {
      alert('View count must be a positive number. Use Revoke button to remove access.');
      return;
    }

    try {
      await ownerAdjustViews(viewer, image, newViews);
      await fetchData(); // Refresh the list
      alert(`Successfully adjusted ${viewer}'s access to ${image} to ${newViews} views`);
    } catch (error) {
      console.error('Failed to adjust views:', error);
      alert('Failed to adjust view count. The viewer might be offline.');
    }
  };

  const handleRevoke = async (viewer: string, image: string) => {
    if (!confirm(`Revoke ${viewer}'s access to ${image}?\n\nThis will permanently delete their copy of the image.`)) {
      return;
    }

    try {
      await ownerRevokeAccess(viewer, image);
      await fetchData(); // Refresh the list
      alert(`Successfully revoked ${viewer}'s access to ${image}`);
    } catch (error) {
      console.error('Failed to revoke access:', error);
      alert('Failed to revoke access. The viewer might be offline.');
    }
  };

  const handleApproveRequest = async (requestId: number, viewer: string, image: string, requestedViews: number) => {
    const approvedCount = prompt(`${viewer} requests ${requestedViews} views for ${image}\n\nApprove how many views?`, requestedViews.toString());
    if (approvedCount === null) return; // User cancelled

    const approved = parseInt(approvedCount);
    if (isNaN(approved) || approved < 0) {
      alert('Please enter a valid number (0 or more)');
      return;
    }

    try {
      await approveAdjustRequest(requestId, approved);
      await fetchData(); // Refresh the list
      alert(`Approved ${viewer}'s request with ${approved} views`);
    } catch (error) {
      console.error('Failed to approve request:', error);
      alert('Failed to approve request. The viewer might be offline.');
    }
  };

  const handleRejectRequest = async (requestId: number, viewer: string, image: string) => {
    const reason = prompt(`Reject ${viewer}'s request for ${image}\n\nReason (optional):`, 'Request denied');
    if (reason === null) return; // User cancelled

    try {
      await rejectAdjustRequest(requestId, reason || 'Request denied');
      await fetchData(); // Refresh the list
      alert(`Rejected ${viewer}'s request`);
    } catch (error) {
      console.error('Failed to reject request:', error);
      alert('Failed to reject request.');
    }
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <StatusBar />

      <div className="container mx-auto px-4 py-6">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-3xl font-bold text-gray-800">Permissions Management</h1>
          <Link to="/dashboard" className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-md">
            Back to Dashboard
          </Link>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <div className="text-gray-500">Loading permissions...</div>
          </div>
        ) : (
          <>
            {/* Pending Adjust Requests Section */}
            {incomingRequests.length > 0 && (
              <div className="mb-8">
                <h2 className="text-2xl font-bold text-gray-800 mb-4">Pending Adjust Requests</h2>
                <div className="bg-yellow-50 border border-yellow-200 rounded-lg shadow overflow-hidden">
                  <table className="min-w-full divide-y divide-yellow-200">
                    <thead className="bg-yellow-100">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Viewer
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Image
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Current Views
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Requested Views
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Received
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-700 uppercase tracking-wider">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-yellow-200">
                      {incomingRequests.map((request) => (
                        <tr key={request.request_id} className="hover:bg-yellow-50">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {request.viewer}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            {request.image_name}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            {request.current_views}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-bold text-blue-700">
                            {request.requested_views}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            {new Date(request.timestamp * 1000).toLocaleString()}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium space-x-2">
                            <button
                              onClick={() => handleApproveRequest(request.request_id, request.viewer, request.image_name, request.requested_views)}
                              className="bg-green-500 hover:bg-green-600 text-white px-3 py-1 rounded-md"
                            >
                              Approve
                            </button>
                            <button
                              onClick={() => handleRejectRequest(request.request_id, request.viewer, request.image_name)}
                              className="bg-red-500 hover:bg-red-600 text-white px-3 py-1 rounded-md"
                            >
                              Reject
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Active Grants Section */}
            <div>
              <h2 className="text-2xl font-bold text-gray-800 mb-4">Active Grants</h2>
              {grants.length === 0 ? (
                <div className="bg-white rounded-lg shadow p-12 text-center">
                  <div className="text-gray-500 text-lg mb-4">No active permissions</div>
                  <p className="text-gray-400 mb-6">
                    When you approve view requests, they will appear here
                  </p>
                  <Link to="/dashboard" className="text-blue-500 hover:text-blue-600">
                    Go to Dashboard
                  </Link>
                </div>
              ) : (
                <div className="bg-white rounded-lg shadow overflow-hidden">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Viewer
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Image
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Granted Views
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Granted At
                        </th>
                        <th className="px-6 py-3 text-left text-xs font-medium text-gray-500 uppercase tracking-wider">
                          Actions
                        </th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {grants.map((grant, index) => (
                        <tr key={index} className="hover:bg-gray-50">
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium text-gray-900">
                            {grant.viewer}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            {grant.image_name}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            <span className={`px-2 py-1 rounded ${grant.granted_views > 3 ? 'bg-green-100 text-green-800' : 'bg-yellow-100 text-yellow-800'}`}>
                              {grant.granted_views} {grant.granted_views === 1 ? 'view' : 'views'}
                            </span>
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm text-gray-700">
                            {new Date(grant.granted_at * 1000).toLocaleString()}
                          </td>
                          <td className="px-6 py-4 whitespace-nowrap text-sm font-medium space-x-2">
                            <button
                              onClick={() => handleAdjust(grant.viewer, grant.image_name, grant.granted_views)}
                              className="bg-blue-500 hover:bg-blue-600 text-white px-3 py-1 rounded-md"
                            >
                              Adjust
                            </button>
                            <button
                              onClick={() => handleRevoke(grant.viewer, grant.image_name)}
                              className="bg-red-500 hover:bg-red-600 text-white px-3 py-1 rounded-md"
                            >
                              Revoke
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </div>
  );
}
