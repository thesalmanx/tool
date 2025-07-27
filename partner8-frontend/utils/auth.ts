// utils/auth.ts
export const setAuthCookies = (userData: any, token: string) => {
  const expires = new Date()
  expires.setDate(expires.getDate() + 7) // 7 days
  
  document.cookie = `access_token=${token}; path=/; expires=${expires.toUTCString()}; secure; samesite=strict`
  document.cookie = `username=${userData.username}; path=/; expires=${expires.toUTCString()}; secure; samesite=strict`
  document.cookie = `user_role=${userData.role}; path=/; expires=${expires.toUTCString()}; secure; samesite=strict`
  document.cookie = `user_id=${userData.id}; path=/; expires=${expires.toUTCString()}; secure; samesite=strict`
  document.cookie = `user_email=${userData.email}; path=/; expires=${expires.toUTCString()}; secure; samesite=strict`
}

export const clearAuthCookies = () => {
  const expiredDate = "Thu, 01 Jan 1970 00:00:01 GMT"
  document.cookie = `access_token=; path=/; expires=${expiredDate}`
  document.cookie = `username=; path=/; expires=${expiredDate}`  
  document.cookie = `user_role=; path=/; expires=${expiredDate}`
  document.cookie = `user_id=; path=/; expires=${expiredDate}`
  document.cookie = `user_email=; path=/; expires=${expiredDate}`
}

export const getCookie = (name: string): string | null => {
  const value = `; ${document.cookie}`
  const parts = value.split(`; ${name}=`)
  if (parts.length === 2) return parts.pop()?.split(';').shift() || null
  return null
}