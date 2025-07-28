// utils/api.js

function getApiUrl() {
  if (typeof window === 'undefined') {
    return process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
  }

  const currentHost = window.location.hostname;
  const protocol = window.location.protocol;

  if (currentHost === 'localhost' || currentHost === '127.0.0.1') {
    return `http://localhost:8000`;
  } else {
    return `${protocol}//${currentHost}`;
  }
}

import { getCookie, clearAuthCookies } from './cookies';

class ApiClient {
  constructor() {
    this.baseURL = getApiUrl();
    this.cache = new Map();
  }

  async request(method, endpoint, data = null, headers = {}, useCache = false) {
    const url = `${this.baseURL}${endpoint}`;
    
    // Always include Authorization header if token exists
    const token = getCookie('access_token');
    const config = {
      method: method,
      headers: {
        'Content-Type': 'application/json',
        ...(token && { 'Authorization': `Bearer ${token}` }),
        ...headers,
      },
    };

    if (data) {
      config.body = JSON.stringify(data);
    }

    const cacheKey = `${method}-${url}-${JSON.stringify(data)}`;

    if (useCache && this.cache.has(cacheKey)) {
      return this.cache.get(cacheKey);
    }

    try {
      const response = await fetch(url, config);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: response.statusText }));
        
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
      throw error;
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
}

const apiClient = new ApiClient();

export { getApiUrl, apiClient };