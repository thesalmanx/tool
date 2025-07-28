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

// Unified cookie utilities
const getCookie = (name) => {
  if (typeof document === 'undefined') return null;
  try {
    const value = `; ${document.cookie}`;
    const parts = value.split(`; ${name}=`);
    if (parts.length === 2) return decodeURIComponent(parts.pop()?.split(';').shift() || '');
    return null;
  } catch (error) {
    console.error('Error getting cookie:', error);
    return null;
  }
};

class ApiClient {
  constructor() {
    this.baseURL = getApiUrl();
    this.cache = new Map();
    console.log(`API Client initialized with base URL: ${this.baseURL}`);
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
      console.log(`Cache hit for ${cacheKey}`);
      return this.cache.get(cacheKey);
    }

    try {
      const response = await fetch(url, config);

      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: response.statusText }));
        
        // Handle 401 errors by clearing cookies and redirecting
        if (response.status === 401) {
          this.clearAuthCookies();
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

  clearAuthCookies() {
    if (typeof document === 'undefined') return;
    const authCookies = ["access_token", "username", "user_role", "user_id", "user_email"];
    authCookies.forEach(cookie => {
      document.cookie = `${cookie}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`;
    });
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

export { getApiUrl, apiClient, getCookie };