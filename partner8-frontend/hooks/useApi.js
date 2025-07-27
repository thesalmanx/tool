// hooks/useApi.js
import { useMemo } from 'react';
import { apiClient } from '../utils/api';

export function useApi() {
  return useMemo(() => apiClient, []);
}
