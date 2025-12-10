import axios from 'axios';

export const API_BASE_URL = 'http://localhost:3001';

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
  online: boolean;
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

// Adjust and Revoke Types
export interface AdjustRequestPayload {
  requested_views: number;
}

export interface AdjustRequestResponse {
  success: boolean;
  request_id: number;
}

export interface LocalAccessGrant {
  viewer: string;
  image_name: string;
  granted_views: number;
  granted_at: number;
}

export interface LocalAccessMapResponse {
  grants: LocalAccessGrant[];
}

export interface OwnerAdjustPayload {
  new_views: number;
}

export interface SuccessResponse {
  success: boolean;
}

export interface IncomingAdjustRequest {
  request_id: number;
  viewer: string;
  image_name: string;
  requested_views: number;
  current_views: number;
  timestamp: number;
}

export interface IncomingAdjustRequestsResponse {
  requests: IncomingAdjustRequest[];
}

export interface ApproveAdjustPayload {
  approved_views: number;
}

export interface RejectAdjustPayload {
  reason: string;
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

// Adjust and Revoke API Methods
export const requestAdjustViews = async (owner: string, image: string, requestedViews: number): Promise<AdjustRequestResponse> => {
  const response = await api.post<AdjustRequestResponse>(`/api/adjust-request/${owner}/${image}`, {
    requested_views: requestedViews,
  });
  return response.data;
};

export const getLocalAccessMap = async (): Promise<LocalAccessMapResponse> => {
  const response = await api.get<LocalAccessMapResponse>('/api/local-access-map');
  return response.data;
};

export const ownerAdjustViews = async (viewer: string, image: string, newViews: number): Promise<SuccessResponse> => {
  const response = await api.post<SuccessResponse>(`/api/owner/adjust/${viewer}/${image}`, {
    new_views: newViews,
  });
  return response.data;
};

export const ownerRevokeAccess = async (viewer: string, image: string): Promise<SuccessResponse> => {
  const response = await api.post<SuccessResponse>(`/api/owner/revoke/${viewer}/${image}`);
  return response.data;
};

export const getIncomingAdjustRequests = async (): Promise<IncomingAdjustRequestsResponse> => {
  const response = await api.get<IncomingAdjustRequestsResponse>('/api/incoming-adjust-requests');
  return response.data;
};

export const approveAdjustRequest = async (requestId: number, approvedViews: number): Promise<SuccessResponse> => {
  const response = await api.post<SuccessResponse>(`/api/owner/approve-adjust/${requestId}`, {
    approved_views: approvedViews,
  });
  return response.data;
};

export const rejectAdjustRequest = async (requestId: number, reason: string): Promise<SuccessResponse> => {
  const response = await api.post<SuccessResponse>(`/api/owner/reject-adjust/${requestId}`, {
    reason,
  });
  return response.data;
};

export default api;
