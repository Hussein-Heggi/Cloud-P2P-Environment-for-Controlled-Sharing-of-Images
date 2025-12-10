import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatusBar from '../components/StatusBar';
import { getLocalAccessMap, ownerAdjustViews, ownerRevokeAccess, LocalAccessGrant } from '../api/client';

export default function Permissions() {
  const [grants, setGrants] = useState<LocalAccessGrant[]>([]);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    fetchGrants();

    // Auto-refresh every 5 seconds
    const interval = setInterval(fetchGrants, 5000);
    return () => clearInterval(interval);
  }, []);

  const fetchGrants = async () => {
    try {
      const data = await getLocalAccessMap();
      setGrants(data.grants);
      setLoading(false);
    } catch (error) {
      console.error('Failed to fetch local access map:', error);
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
      await fetchGrants(); // Refresh the list
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
      await fetchGrants(); // Refresh the list
      alert(`Successfully revoked ${viewer}'s access to ${image}`);
    } catch (error) {
      console.error('Failed to revoke access:', error);
      alert('Failed to revoke access. The viewer might be offline.');
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
        ) : grants.length === 0 ? (
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
                    Remaining Views
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
    </div>
  );
}
