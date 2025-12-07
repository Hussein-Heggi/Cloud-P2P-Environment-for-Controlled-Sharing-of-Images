import axios from 'axios'

const api = axios.create({
  baseURL: 'http://localhost:3001',
})

// ============================================================================
// Types
// ============================================================================

export interface StatusResponse {
  connected: boolean
  username: string
  dos_version: number
  server_addr: string
}

export interface DosClientInfo {
  name: string
  ip: string
  port: number
  images: string[]
  online: boolean
  last_seen: number
}

export interface DosResponse {
  users: DosClientInfo[]
  version: number
}

export interface ViewRequestPayload {
  owner: string
  image_name: string
  requested_views: number
}

export interface ViewRequestResponse {
  request_id: number
  status: string
}

export interface PendingRequestInfo {
  request_id: number
  owner: string
  image: string
  status: string
  timestamp: number
}

export interface PendingViewNotification {
  request_id: number
  viewer: string
  image: string
  requested_views: number
  timestamp: number
}

export interface DownloadInfo {
  owner: string
  image_name: string
  embedded_path: string
  extracted_path: string | null
  metadata: string | null
}

export interface ExtractionResponse {
  true_image_path: string
  metadata: string
}

export interface ApproveResponse {
  success: boolean
  message: string
}

// ============================================================================
// API Methods
// ============================================================================

export const getStatus = async (): Promise<StatusResponse> => {
  const { data } = await api.get('/api/status')
  return data
}

export const getDos = async (): Promise<DosResponse> => {
  const { data } = await api.get('/api/dos')
  return data
}

export const requestView = async (payload: ViewRequestPayload): Promise<ViewRequestResponse> => {
  const { data } = await api.post('/api/request-view', payload)
  return data
}

export const getMyRequests = async (): Promise<PendingRequestInfo[]> => {
  const { data } = await api.get('/api/requests')
  return data
}

export const getNotifications = async (): Promise<PendingViewNotification[]> => {
  const { data } = await api.get('/api/notifications')
  return data
}

export const approveRequest = async (requestId: number): Promise<ApproveResponse> => {
  const { data } = await api.post(`/api/approve/${requestId}`)
  return data
}

export const denyRequest = async (requestId: number): Promise<ApproveResponse> => {
  const { data } = await api.post(`/api/deny/${requestId}`)
  return data
}

export const getDownloads = async (): Promise<DownloadInfo[]> => {
  const { data } = await api.get('/api/downloads')
  return data
}

export const extractImage = async (filename: string): Promise<ExtractionResponse> => {
  const { data } = await api.post(`/api/extract/${filename}`)
  return data
}

export default api
