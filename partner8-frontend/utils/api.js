// utils/api.js - Updated with form data support and correct port

function getApiUrl() {
  if (typeof window === 'undefined') {
    return process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8100'; // Changed from 8100 to 8000
  }

  const currentHost = window.location.hostname;
  const protocol = window.location.protocol;

  if (currentHost === 'localhost' || currentHost === '127.0.0.1') {
    return `http://localhost:8100`; // Changed from 8100 to 8000
  } else {
    return `${protocol}//${currentHost}:8100`; // Changed from 8100 to 8000
  }
}

import { getCookie, clearAuthCookies } from './cookies';

class ApiClient {
  constructor() {
    this.baseURL = getApiUrl();
    this.cache = new Map();
  }

  async request(method, endpoint, data = null, headers = {}, useCache = false, isFormData = false) {
    const url = `${this.baseURL}${endpoint}`;
    
    // Always include Authorization header if token exists
    const token = getCookie('access_token');
    const config = {
      method: method,
      headers: {
        'Accept': 'application/json',
        ...(token && { 'Authorization': `Bearer ${token}` }),
        ...headers,
      },
      mode: 'cors',
      credentials: 'include',
    };

    // Handle different data types
    if (data) {
      if (isFormData) {
        // Don't set Content-Type for FormData - let browser set it with boundary
        config.body = data;
      } else {
        // Set Content-Type for JSON data
        config.headers['Content-Type'] = 'application/json';
        config.body = JSON.stringify(data);
      }
    }

    const cacheKey = `${method}-${url}-${JSON.stringify(data)}`;

    if (useCache && this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    try {
      const response = await fetch(url, config);

      if (!response.ok) {
        let errorData;
        const contentType = response.headers.get('content-type');
        
        if (contentType && contentType.includes('application/json')) {
          try {
            errorData = await response.json();
          } catch {
            errorData = { detail: `HTTP ${response.status}: ${response.statusText}` };
          }
        } else {
          const errorText = await response.text().catch(() => response.statusText);
          errorData = { detail: errorText || `HTTP ${response.status}: ${response.statusText}` };
        }
        
        // Handle 401 errors by clearing cookies and redirecting
        if (response.status === 401) {
          clearAuthCookies();
          if (typeof window !== 'undefined') {
            window.location.href = '/';
          }
        }
        
        throw new Error(errorData.detail || `API Error: ${response.status}`);
      }

      const result = await response.json();
      if (useCache) {
        this.cache.set(cacheKey, result);
      }
      return result;
    } catch (error) {
      console.error(`API Request Error (${method} ${url}):`, error);
      
      // Handle different types of errors
      if (error.name === 'AbortError') {
        throw new Error('Request timeout - please try again');
      } else if (error.message.includes('NetworkError') || error.message.includes('Failed to fetch')) {
        throw new Error('Cannot connect to server. Please ensure the backend is running on http://localhost:8000');
      } else {
        throw error;
      }
    }
  }

  get(endpoint, headers = {}, useCache = false) {
    return this.request('GET', endpoint, null, headers, useCache);
  }

  post(endpoint, data, headers = {}) {
    return this.request('POST', endpoint, data, headers);
  }

  put(endpoint, data, headers = {}) {
    return this.request('PUT', endpoint, data, headers);
  }

  delete(endpoint, headers = {}) {
    return this.request('DELETE', endpoint, null, headers);
  }

  // Special method for form data (like login)
  postForm(endpoint, formData, headers = {}) {
    return this.request('POST', endpoint, formData, headers, false, true);
  }

  // Login method that handles OAuth2 form data
  async login(username, password) {
    const formData = new FormData();
    formData.append('username', username.trim());
    formData.append('password', password);
    
    return this.postForm('/token', formData);
  }

  // Health check method
  async healthCheck() {
    try {
      const response = await this.get('/health');
      return { success: true, data: response };
    } catch (error) {
      return { success: false, error: error.message };
    }
  }
}

const apiClient = new ApiClient();

export { getApiUrl, apiClient };