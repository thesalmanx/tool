// utils/cookies.ts - Create this new file for consistent cookie handling

// Unified cookie utilities for the entire app
export const setCookie = (name: string, value: string, days: number = 7) => {
  try {
    const expires = new Date()
    expires.setTime(expires.getTime() + days * 24 * 60 * 60 * 1000)
    // Encode the value when setting
    document.cookie = `${name}=${encodeURIComponent(value)};expires=${expires.toUTCString()};path=/;samesite=lax`
    console.log(`Cookie set: ${name}`)
  } catch (error) {
    console.error('Error setting cookie:', error)
  }
}

export const getCookie = (name: string): string | null => {
  if (typeof document === 'undefined') return null;
  try {
    const value = `; ${document.cookie}`
    const parts = value.split(`; ${name}=`)
    if (parts.length === 2) {
      const cookieValue = parts.pop()?.split(';').shift() || ''
      // Decode the value when getting
      return decodeURIComponent(cookieValue)
    }
    return null
  } catch (error) {
    console.error('Error getting cookie:', error)
    return null
  }
}

export const deleteCookie = (name: string) => {
  try {
    document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/; samesite=lax`
  } catch (error) {
    console.error('Error deleting cookie:', error)
  }
}

export const clearAuthCookies = () => {
  try {
    const authCookies = ["access_token", "username", "user_role", "user_id", "user_email"]
    authCookies.forEach(cookie => deleteCookie(cookie))
    console.log("Auth cookies cleared")
  } catch (error) {
    console.error('Error clearing auth cookies:', error)
  }
}