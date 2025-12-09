import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatusBar from '../components/StatusBar';
import { getViewerAccessMap, viewImage, AccessGrant } from '../api/client';

export default function Downloads() {
  const [grants, setGrants] = useState<AccessGrant[]>([]);
  const [loading, setLoading] = useState(true);
  const [viewing, setViewing] = useState<{ owner: string; image: string } | null>(null);
  const [currentImage, setCurrentImage] = useState<{ url: string; owner: string; image: string; remainingViews: number } | null>(null);

  useEffect(() => {
    fetchGrants();
  }, []);

  const fetchGrants = async () => {
    try {
      const data = await getViewerAccessMap();
      setGrants(data.grants);
      setLoading(false);
    } catch (error) {
      console.error('Failed to fetch viewer access map:', error);
      setLoading(false);
    }
  };

  const handleViewImage = async (owner: string, imageName: string) => {
    setViewing({ owner, image: imageName });
    try {
      const { blob, remainingViews } = await viewImage(owner, imageName);

      // Create object URL from blob
      const imageUrl = URL.createObjectURL(blob);

      setCurrentImage({
        url: imageUrl,
        owner,
        image: imageName,
        remainingViews,
      });

      // Update the grant in the list
      setGrants(prevGrants =>
        prevGrants.map(g =>
          g.owner === owner && g.image_name === imageName
            ? { ...g, remaining_views: remainingViews }
            : g
        ).filter(g => g.remaining_views > 0) // Remove grants with 0 views
      );
    } catch (error) {
      console.error('Failed to view image:', error);
      alert('Failed to view image. You may have no remaining views.');
      await fetchGrants(); // Refresh the list
    } finally {
      setViewing(null);
    }
  };

  const closeImageViewer = () => {
    if (currentImage) {
      URL.revokeObjectURL(currentImage.url);
    }
    setCurrentImage(null);
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <StatusBar />

      <div className="container mx-auto px-4 py-6">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-3xl font-bold text-gray-800">My Images</h1>
          <Link to="/dashboard" className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-md">
            Back to Dashboard
          </Link>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <div className="text-gray-500">Loading your images...</div>
          </div>
        ) : grants.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-12 text-center">
            <div className="text-gray-500 text-lg mb-4">No images available</div>
            <p className="text-gray-400 mb-6">Request images from the Dashboard to view them here</p>
            <Link to="/dashboard" className="text-blue-500 hover:text-blue-600">
              Go to Dashboard
            </Link>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {grants.map((grant, index) => (
              <div key={index} className="bg-white rounded-lg shadow overflow-hidden">
                <div className="p-4">
                  <h3 className="font-semibold text-lg mb-2">{grant.image_name}</h3>
                  <p className="text-sm text-gray-600 mb-1">
                    <strong>Owner:</strong> {grant.owner}
                  </p>
                  <p className="text-sm text-gray-600 mb-1">
                    <strong>Remaining Views:</strong>{' '}
                    <span className={`font-bold ${
                      grant.remaining_views <= 3 ? 'text-red-600' : 'text-green-600'
                    }`}>
                      {grant.remaining_views}
                    </span>
                  </p>
                  <p className="text-xs text-gray-500 mb-4">
                    Received: {new Date(grant.received_at * 1000).toLocaleString()}
                  </p>

                  {/* View Button */}
                  <button
                    onClick={() => handleViewImage(grant.owner, grant.image_name)}
                    disabled={viewing?.owner === grant.owner && viewing?.image === grant.image_name}
                    className={`w-full py-2 px-4 rounded-md font-medium ${
                      viewing?.owner === grant.owner && viewing?.image === grant.image_name
                        ? 'bg-gray-300 text-gray-600 cursor-not-allowed'
                        : grant.remaining_views <= 0
                        ? 'bg-red-500 text-white cursor-not-allowed'
                        : 'bg-blue-500 hover:bg-blue-600 text-white'
                    }`}
                  >
                    {viewing?.owner === grant.owner && viewing?.image === grant.image_name
                      ? 'Loading...'
                      : grant.remaining_views <= 0
                      ? 'No Views Left'
                      : 'View Image'}
                  </button>
                </div>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Image Viewer Modal */}
      {currentImage && (
        <div className="fixed inset-0 bg-black bg-opacity-75 flex items-center justify-center p-4 z-50">
          <div className="bg-white rounded-lg max-w-4xl max-h-full overflow-auto">
            <div className="p-4 border-b border-gray-200 flex items-center justify-between">
              <div>
                <h3 className="text-lg font-semibold">{currentImage.image}</h3>
                <p className="text-sm text-gray-600">
                  Owner: {currentImage.owner} | Remaining Views: <span className="font-bold text-green-600">{currentImage.remainingViews}</span>
                </p>
              </div>
              <button
                onClick={closeImageViewer}
                className="text-gray-500 hover:text-gray-700 text-2xl font-bold"
              >
                ×
              </button>
            </div>
            <div className="p-4">
              <img
                src={currentImage.url}
                alt={currentImage.image}
                className="max-w-full h-auto"
              />
            </div>
            <div className="p-4 border-t border-gray-200 bg-yellow-50">
              <p className="text-sm text-yellow-800">
                ⚠️ This view has been counted. You have <strong>{currentImage.remainingViews}</strong> view{currentImage.remainingViews !== 1 ? 's' : ''} remaining.
              </p>
            </div>
            <div className="p-4 border-t border-gray-200">
              <button
                onClick={closeImageViewer}
                className="w-full bg-gray-500 hover:bg-gray-600 text-white py-2 px-4 rounded-md font-medium"
              >
                Close
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
