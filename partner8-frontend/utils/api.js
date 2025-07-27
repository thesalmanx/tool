// utils/api.js

function getApiUrl() {
  if (typeof window === 'undefined') {
    // Server-side rendering or build time
    // You might need to adjust this if your backend is not on localhost during build
    return process.env.NEXT_PUBLIC_BACKEND_URL || 'http://localhost:8000';
  }

  // Client-side
  const currentHost = window.location.hostname;
  const currentPort = window.location.port;
  const protocol = window.location.protocol;

  if (currentHost === 'localhost' || currentHost === '127.0.0.1') {
    return `http://localhost:8000`; // Development backend URL
  } else {
    // Production: Assume backend is on the same domain but on port 8000
    // Adjust port if your production backend uses a different port or sub-domain
    return `${protocol}//${currentHost}`;
  }
}

class ApiClient {
  constructor() {
    this.baseURL = getApiUrl();
    this.cache = new Map(); // Basic caching mechanism
    console.log(`API Client initialized with base URL: ${this.baseURL}`);
  }

  async request(method, endpoint, data = null, headers = {}, useCache = false) {
    const url = `${this.baseURL}${endpoint}`;
    const config = {
      method: method,
      headers: {
        'Content-Type': 'application/json',
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
