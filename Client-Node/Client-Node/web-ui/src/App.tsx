import { useState } from 'react'
import { useNavigate } from 'react-router-dom'

function App() {
  const navigate = useNavigate()
  const [username, setUsername] = useState('')
  const [serverAddr, setServerAddr] = useState('10.40.61.79:9080')
  const [connecting, setConnecting] = useState(false)
  const [error, setError] = useState('')

  const handleConnect = async () => {
    if (!username.trim()) {
      setError('Please enter a username')
      return
    }

    setConnecting(true)
    setError('')

    try {
      // Store connection info in localStorage
      localStorage.setItem('username', username)
      localStorage.setItem('serverAddr', serverAddr)
      
      // Navigate to dashboard
      navigate('/dashboard')
    } catch (err: any) {
      setError(err.message || 'Failed to connect')
    } finally {
      setConnecting(false)
    }
  }

  return (
    <div className="min-h-screen bg-gradient-to-br from-blue-900 via-purple-900 to-indigo-900 flex items-center justify-center p-4">
      <div className="bg-white/10 backdrop-blur-lg rounded-2xl shadow-2xl p-8 w-full max-w-md border border-white/20">
        <h1 className="text-4xl font-bold text-white mb-2 text-center">Cloud-P2P</h1>
        <p className="text-blue-200 text-center mb-8">Secure Image Sharing</p>

        <div className="space-y-6">
          <div>
            <label className="block text-sm font-medium text-blue-100 mb-2">
              Username
            </label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-transparent"
              placeholder="Enter your username"
              onKeyPress={(e) => e.key === 'Enter' && handleConnect()}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-blue-100 mb-2">
              Server Address
            </label>
            <input
              type="text"
              value={serverAddr}
              onChange={(e) => setServerAddr(e.target.value)}
              className="w-full px-4 py-3 bg-white/10 border border-white/30 rounded-lg text-white placeholder-blue-200 focus:outline-none focus:ring-2 focus:ring-blue-400 focus:border-transparent"
              placeholder="10.40.61.79:9080"
            />
          </div>

          {error && (
            <div className="bg-red-500/20 border border-red-500/50 rounded-lg p-3 text-red-200 text-sm">
              {error}
            </div>
          )}

          <button
            onClick={handleConnect}
            disabled={connecting}
            className="w-full bg-gradient-to-r from-blue-500 to-purple-600 hover:from-blue-600 hover:to-purple-700 disabled:from-gray-500 disabled:to-gray-600 text-white font-semibold py-3 px-6 rounded-lg transition-all duration-200 shadow-lg hover:shadow-xl disabled:cursor-not-allowed"
          >
            {connecting ? 'Connecting...' : 'Connect to Network'}
          </button>

          <div className="text-xs text-blue-200 text-center mt-4">
            <p>Make sure the Rust client is running in interactive mode:</p>
            <code className="block mt-2 bg-black/30 px-3 py-2 rounded">
              cargo run -- interactive {username || 'USERNAME'} {serverAddr}
            </code>
          </div>
        </div>
      </div>
    </div>
  )
}

export default App
