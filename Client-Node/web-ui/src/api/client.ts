import axios from 'axios';

const API_BASE_URL = 'http://localhost:3001';

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Types matching the Rust backend (MINIMAL DOS-C v2.0)
export interface DosClient {
  name: string;
  images: string[];
  // Removed: ip, port, online, last_seen (not in minimal DOS-C)
}

export interface DosResponse {
  users: DosClient[];
  version: number;
}

export interface StatusResponse {
  connected: boolean;
  username: string;
  dos_version: number;
  server_addr: string;
}

export interface ViewRequestPayload {
  owner: string;
  image_name: string;
  requested_views: number;
}

export interface ViewRequestResponse {
  request_id: number;
  status: string;
}

export interface PendingRequest {
  request_id: number;
  owner: string;
  image: string;
  status: string;
  timestamp: number;
}

export interface PendingViewNotification {
  request_id: number;
  viewer: string;
  image: string;
  requested_views: number;
  timestamp: number;
}

export interface DownloadInfo {
  owner: string;
  image_name: string;
  embedded_path: string;
  extracted_path?: string;
  metadata?: string;
}

export interface ExtractionResponse {
  true_image_path: string;
  metadata: string;
}

export interface ApproveResponse {
  success: boolean;
  message: string;
}

export interface AccessGrant {
  owner: string;
  image_name: string;
  remaining_views: number;
  received_at: number;
  encrypted_path: string;
}

export interface ViewerAccessMapResponse {
  grants: AccessGrant[];
  total_available: number;
}

// API Methods
export const getStatus = async (): Promise<StatusResponse> => {
  const response = await api.get<StatusResponse>('/api/status');
  return response.data;
};

export const getDos = async (): Promise<DosResponse> => {
  const response = await api.get<DosResponse>('/api/dos');
  return response.data;
};

export const requestView = async (payload: ViewRequestPayload): Promise<ViewRequestResponse> => {
  const response = await api.post<ViewRequestResponse>('/api/request-view', payload);
  return response.data;
};

export const getRequests = async (): Promise<PendingRequest[]> => {
  const response = await api.get<PendingRequest[]>('/api/requests');
  return response.data;
};

export const getNotifications = async (): Promise<PendingViewNotification[]> => {
  const response = await api.get<PendingViewNotification[]>('/api/notifications');
  return response.data;
};

export const approveRequest = async (requestId: number): Promise<ApproveResponse> => {
  const response = await api.post<ApproveResponse>(`/api/approve/${requestId}`);
  return response.data;
};

export const denyRequest = async (requestId: number): Promise<ApproveResponse> => {
  const response = await api.post<ApproveResponse>(`/api/deny/${requestId}`);
  return response.data;
};

export const getDownloads = async (): Promise<DownloadInfo[]> => {
  const response = await api.get<DownloadInfo[]>('/api/downloads');
  return response.data;
};

export const extractImage = async (filename: string): Promise<ExtractionResponse> => {
  const response = await api.post<ExtractionResponse>(`/api/extract/${filename}`);
  return response.data;
};

export const getViewerAccessMap = async (): Promise<ViewerAccessMapResponse> => {
  const response = await api.get<ViewerAccessMapResponse>('/api/viewer-access-map');
  return response.data;
};

export const viewImage = async (owner: string, imageName: string): Promise<{ blob: Blob; remainingViews: number }> => {
  const response = await api.get(`/api/view/${owner}/${imageName}`, {
    responseType: 'blob',
  });

  // Extract remaining views from response header
  const remainingViews = parseInt(response.headers['x-remaining-views'] || '0', 10);

  return {
    blob: response.data,
    remainingViews,
  };
};

export default api;
