import { NextResponse } from "next/server"
import type { NextRequest } from "next/server"

export async function middleware(request: NextRequest) {
  const path = request.nextUrl.pathname
  const isPublicPath = path === "/" || path === "/login" || path === "/signup"

  const token = request.cookies.get("access_token")?.value
  console.log(`Middleware: ${path}, Token: ${token ? "exists" : "none"}`)

  let response: NextResponse

  if (token) {
    try {
      const backendUrl = process.env.NEXT_PUBLIC_BACKEND_URL || "http://localhost:8000"

      const verificationResponse = await fetch(`${backendUrl}/verify-token`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
        signal: AbortSignal.timeout(5000), // Ensure timeout to avoid hanging
      })

      if (verificationResponse.ok) {
        const data = await verificationResponse.json()
        console.log("Token verified, user:", data.user.username)

        if (isPublicPath) {
          console.log("Redirecting authenticated user to /dashboard")
          return NextResponse.redirect(new URL("/dashboard", request.nextUrl))
        }

        // Authenticated and on protected route
        return NextResponse.next()
      } else {
        console.warn("Token verification failed. Redirecting to login.")
        response = NextResponse.redirect(new URL("/", request.nextUrl))
      }
    } catch (error) {
      console.error("Token verification error:", error)
      response = NextResponse.redirect(new URL("/", request.nextUrl))
    }

    // Clear cookies after failure or error
    response.cookies.delete("access_token")
    response.cookies.delete("username")
    response.cookies.delete("user_role")
    response.cookies.delete("user_id")
    response.cookies.delete("user_email")
    return response
  }

  // No token
  if (!isPublicPath) {
    console.log("Unauthenticated user trying to access protected route. Redirecting.")
    return NextResponse.redirect(new URL("/", request.nextUrl))
  }

  // Public route and no token → allow access
  return NextResponse.next()
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
