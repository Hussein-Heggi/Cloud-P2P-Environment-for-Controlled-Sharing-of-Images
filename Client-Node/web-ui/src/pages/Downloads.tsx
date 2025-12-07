import { useState, useEffect } from 'react';
import { Link } from 'react-router-dom';
import StatusBar from '../components/StatusBar';
import { getDownloads, extractImage, DownloadInfo } from '../api/client';

export default function Downloads() {
  const [downloads, setDownloads] = useState<DownloadInfo[]>([]);
  const [loading, setLoading] = useState(true);
  const [extracting, setExtracting] = useState<string | null>(null);

  useEffect(() => {
    fetchDownloads();
  }, []);

  const fetchDownloads = async () => {
    try {
      const data = await getDownloads();
      setDownloads(data);
      setLoading(false);
    } catch (error) {
      console.error('Failed to fetch downloads:', error);
      setLoading(false);
    }
  };

  const handleExtract = async (filename: string) => {
    setExtracting(filename);
    try {
      const result = await extractImage(filename);
      alert(`Extraction successful!\nTrue image: ${result.true_image_path}`);
      // Refresh downloads list
      await fetchDownloads();
    } catch (error) {
      console.error('Failed to extract:', error);
      alert('Extraction failed');
    } finally {
      setExtracting(null);
    }
  };

  const getFilename = (path: string) => {
    return path.split('/').pop() || path;
  };

  return (
    <div className="min-h-screen bg-gray-100">
      <StatusBar />

      <div className="container mx-auto px-4 py-6">
        <div className="mb-6 flex items-center justify-between">
          <h1 className="text-3xl font-bold text-gray-800">Downloads</h1>
          <Link to="/dashboard" className="bg-blue-500 hover:bg-blue-600 text-white px-4 py-2 rounded-md">
            Back to Dashboard
          </Link>
        </div>

        {loading ? (
          <div className="text-center py-12">
            <div className="text-gray-500">Loading downloads...</div>
          </div>
        ) : downloads.length === 0 ? (
          <div className="bg-white rounded-lg shadow p-12 text-center">
            <div className="text-gray-500 text-lg mb-4">No downloads yet</div>
            <p className="text-gray-400 mb-6">Request images from the Dashboard to see them here</p>
            <Link to="/dashboard" className="text-blue-500 hover:text-blue-600">
              Go to Dashboard
            </Link>
          </div>
        ) : (
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-6">
            {downloads.map((download, index) => (
              <div key={index} className="bg-white rounded-lg shadow overflow-hidden">
                <div className="p-4">
                  <h3 className="font-semibold text-lg mb-2">{download.image_name}</h3>
                  <p className="text-sm text-gray-600 mb-1">
                    <strong>Owner:</strong> {download.owner}
                  </p>
                  <p className="text-xs text-gray-500 mb-4">
                    Embedded: {getFilename(download.embedded_path)}
                  </p>

                  {/* Embedded Image Preview */}
                  <div className="mb-4">
                    <p className="text-xs font-medium text-gray-700 mb-2">Embedded Image (Cover):</p>
                    <div className="border border-gray-200 rounded overflow-hidden">
                      <img
                        src={`http://localhost:3001/${download.embedded_path}`}
                        alt="Embedded"
                        className="w-full h-48 object-cover"
                        onError={(e) => {
                          (e.target as HTMLImageElement).src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="100" height="100"%3E%3Crect fill="%23ddd" width="100" height="100"/%3E%3Ctext x="50%25" y="50%25" dominant-baseline="middle" text-anchor="middle" fill="%23999"%3ENo Image%3C/text%3E%3C/svg%3E';
                        }}
                      />
                    </div>
                  </div>

                  {/* Extract Button or True Image */}
                  {!download.extracted_path ? (
                    <button
                      onClick={() => handleExtract(getFilename(download.embedded_path))}
                      disabled={extracting === getFilename(download.embedded_path)}
                      className={`w-full py-2 px-4 rounded-md font-medium ${
                        extracting === getFilename(download.embedded_path)
                          ? 'bg-gray-300 text-gray-600 cursor-not-allowed'
                          : 'bg-green-500 hover:bg-green-600 text-white'
                      }`}
                    >
                      {extracting === getFilename(download.embedded_path) ? 'Extracting...' : 'Extract True Image'}
                    </button>
                  ) : (
                    <div>
                      <p className="text-xs font-medium text-green-700 mb-2">
                        ✓ Extracted: {getFilename(download.extracted_path)}
                      </p>
                      <div className="border border-green-200 rounded overflow-hidden">
                        <img
                          src={`http://localhost:3001/${download.extracted_path}`}
                          alt="True Image"
                          className="w-full h-48 object-cover"
                          onError={(e) => {
                            (e.target as HTMLImageElement).src = 'data:image/svg+xml,%3Csvg xmlns="http://www.w3.org/2000/svg" width="100" height="100"%3E%3Crect fill="%23ddd" width="100" height="100"/%3E%3Ctext x="50%25" y="50%25" dominant-baseline="middle" text-anchor="middle" fill="%23999"%3ENo Image%3C/text%3E%3C/svg%3E';
                          }}
                        />
                      </div>
                    </div>
                  )}

                  {/* Metadata */}
                  {download.metadata && (
                    <details className="mt-4">
                      <summary className="cursor-pointer text-xs text-blue-600 hover:text-blue-800">
                        View Metadata
                      </summary>
                      <pre className="mt-2 text-xs bg-gray-50 p-2 rounded overflow-x-auto">
                        {download.metadata}
                      </pre>
                    </details>
                  )}
                </div>
              </div>
            ))}
          </div>
        )}
      </div>
    </div>
  );
}
