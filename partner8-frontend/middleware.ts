import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"

export async function middleware(request: NextRequest) {
  const path = request.nextUrl.pathname
  const isPublicPath = path === "/" || path === "/login" || path === "/signup"

  const token = request.cookies.get("access_token")?.value
  console.log(`Middleware: ${path}, Token: ${token ? "exists" : "none"}`)

  // If no token and trying to access protected route
  if (!token && !isPublicPath) {
    console.log("No token, redirecting to /")
    return NextResponse.redirect(new URL("/", request.nextUrl))
  }

  // If token exists, verify it only once
  if (token) {
    try {
      const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000"
      console.log(`Verifying token with backend: ${backendUrl}/verify-token`)

      const verificationResponse = await fetch(`${backendUrl}/verify-token`, {
        headers: {
          Authorization: `Bearer ${token}`,
          'Cache-Control': 'no-cache'
        },
        signal: AbortSignal.timeout(10000), // 10 second timeout
      })

      if (verificationResponse.ok) {
        const data = await verificationResponse.json()
        console.log("Token verified successfully for user:", data.user.username)

        // If authenticated and trying to access public path, redirect to dashboard
        if (isPublicPath) {
          console.log("Authenticated user on public path, redirecting to dashboard")
          return NextResponse.redirect(new URL("/dashboard", request.nextUrl))
        }

        // Set user data in response headers for the frontend to read
        const response = NextResponse.next()
        response.headers.set('x-user-role', data.user.role)
        response.headers.set('x-user-username', data.user.username)
        response.headers.set('x-user-id', data.user.id.toString())
        response.headers.set('x-user-email', data.user.email)
        
        return response
      } else {
        console.warn(`Token verification failed with status: ${verificationResponse.status}`)
        // Clear invalid token and redirect to home
        const response = NextResponse.redirect(new URL("/", request.nextUrl))
        clearAuthCookies(response)
        return response
      }
    } catch (error) {
      console.error("Token verification error:", error)
      // On network error, clear cookies and redirect
      const response = NextResponse.redirect(new URL("/", request.nextUrl))
      clearAuthCookies(response)
      return response
    }
  }

  // Public route and no token → allow access
  return NextResponse.next()
}

function clearAuthCookies(response: NextResponse) {
  const authCookies = ["access_token", "username", "user_role", "user_id", "user_email"]
  authCookies.forEach(cookie => {
    response.cookies.delete(cookie)
  })
}

export const config = {
  matcher: [
    /**
     * Match all request paths except for:
     * - /api
     * - /_next/static
     * - /_next/image
     * - /favicon.ico
     * - /public folder files
     */
    "/((?!api|_next/static|_next/image|favicon.ico|public).*)",
  ],
}