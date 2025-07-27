"use client"

import type React from "react"
import { useState, useEffect } from "react"
import { usePathname, useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet"
import { Home, Users, MessageSquare, LogOut, Menu, Play } from "lucide-react"

interface DashboardLayoutProps {
  children: React.ReactNode
}

// Use the same API base URL as the login form
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "https://investmentapp.partners8.com"

// Helper functions for cookie management
const getCookie = (name: string): string | null => {
  const value = `; ${document.cookie}`
  const parts = value.split(`; ${name}=`)
  if (parts.length === 2) return parts.pop()?.split(';').shift() || null
  return null
}

const setCookie = (name: string, value: string, days: number = 7) => {
  const expires = new Date(Date.now() + days * 864e5).toUTCString()
  document.cookie = `${name}=${value}; expires=${expires}; path=/; SameSite=Strict`
}

const deleteCookie = (name: string) => {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`
}

export default function DashboardLayout({ children }: DashboardLayoutProps) {
  const [userRole, setUserRole] = useState("")
  const [username, setUsername] = useState("")
  const [isLoading, setIsLoading] = useState(true)
  const pathname = usePathname()
  const router = useRouter()

  useEffect(() => {
    const verifyAuth = async () => {
      console.log("DashboardLayout: Verifying authentication...")
      try {
        // Check for token in cookies instead of localStorage
        const token = getCookie("access_token")
        if (!token) {
          console.log("DashboardLayout: No token found, redirecting to /")
          router.replace("/")
          return
        }
        console.log("DashboardLayout: Token found, attempting to verify with backend.")
        console.log("DashboardLayout: Using API URL:", `${API_BASE_URL}/verify-token`)

        const response = await fetch(`${API_BASE_URL}/verify-token`, {
          headers: {
            Authorization: `Bearer ${token}`,
          },
        })

        console.log("DashboardLayout: Verify token response status:", response.status)

        if (response.ok) {
          const data = await response.json()
          console.log("DashboardLayout: Verify token response data:", data)

          if (data.valid && data.user.is_approved) {
            // Store user data in cookies
            setCookie("username", data.user.username)
            setCookie("user_role", data.user.role)
            setCookie("user_id", data.user.id.toString())
            setCookie("user_email", data.user.email)

            setUserRole(data.user.role)
            setUsername(data.user.username)
            
            // Handle role-based redirection
            if (data.user.role === 'user') {
              if (pathname === '/dashboard/scraping') {
                router.replace("/dashboard/chat")
                return
              }
              if (pathname.includes('/dashboard/users') || pathname.includes('/dashboard/scraping')) {
                router.replace("/dashboard/chat")
                return
              }
            }
            
            setIsLoading(false)
            console.log("DashboardLayout: User authenticated and approved.")
          } else {
            console.log("DashboardLayout: Token valid but user not approved, clearing cookies and redirecting")
            clearAllCookies()
            router.replace("/")
          }
        } else if (response.status === 401 || response.status === 403) {
          console.log(`DashboardLayout: Backend returned ${response.status}, clearing cookies and redirecting`)
          clearAllCookies()
          router.replace("/")
        } else {
          console.error("DashboardLayout: Unexpected backend response:", response.status, await response.text())
          clearAllCookies()
          router.replace("/")
        }
      } catch (error) {
        console.error("DashboardLayout: Network or fetch error during authentication:", error)
        clearAllCookies()
        router.replace("/")
      }
    }

    verifyAuth()
  }, [router, pathname])

  const clearAllCookies = () => {
    deleteCookie("access_token")
    deleteCookie("username")
    deleteCookie("user_role")
    deleteCookie("user_id")
    deleteCookie("user_email")
  }

  const handleLogout = () => {
    console.log("DashboardLayout: Logging out...")
    try {
      clearAllCookies()
      router.replace("/")
    } catch (error) {
      console.error("Logout error:", error)
      router.replace("/")
    }
  }

  const navigateToPage = (href: string) => {
    router.push(href)
  }

  if (isLoading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Verifying session...</p>
        </div>
      </div>
    )
  }

  // Define navigation based on user role
  const navigation =
  userRole === "user"
    ? [
        { name: "Dashboard", href: "/dashboard", icon: Home },
        { name: "Chat", href: "/dashboard/chat", icon: MessageSquare },
      ]
    : [
        { name: "Dashboard", href: "/dashboard", icon: Home },
        { name: "Users", href: "/dashboard/users", icon: Users },
        { name: "Scraping", href: "/dashboard/scraping", icon: Play },
        { name: "Chat", href: "/dashboard/chat", icon: MessageSquare },
      ];


  const Sidebar = () => (
    <div className="flex h-full flex-col">
      <div className="flex h-14 items-center border-b px-4">
        <button 
          onClick={() => navigateToPage(userRole === "user" ? "/dashboard/chat" : "/dashboard")} 
          className="flex items-center space-x-2"
        >
          <span className="font-bold">Partners8</span>
        </button>
      </div>
      <nav className="flex-1 space-y-1 p-4">
        {navigation.map((item) => {
          const Icon = item.icon
          const isActive = pathname === item.href
          return (
            <button
              key={item.name}
              onClick={() => navigateToPage(item.href)}
              className={`w-full flex items-center space-x-2 rounded-lg px-3 py-2 text-sm font-medium transition-colors ${
                isActive ? "bg-gray-100 text-gray-900" : "text-gray-600 hover:bg-gray-50 hover:text-gray-900"
              }`}
            >
              <Icon className="h-4 w-4" />
              <span>{item.name}</span>
            </button>
          )
        })}
      </nav>
      <div className="border-t p-4">
        <div className="flex items-center space-x-3 mb-3">
          <div className="h-8 w-8 rounded-full bg-gray-300 flex items-center justify-center">
            <span className="text-sm font-medium">{username.charAt(0).toUpperCase()}</span>
          </div>
          <div>
            <p className="text-sm font-medium">{username}</p>
            <p className="text-xs text-gray-500">{userRole}</p>
          </div>
        </div>
        <Button variant="outline" size="sm" className="w-full bg-transparent" onClick={handleLogout}>
          <LogOut className="h-4 w-4 mr-2" />
          Logout
        </Button>
      </div>
    </div>
  )

  return (
    <div className="flex h-screen bg-gray-50">
      {/* Desktop Sidebar */}
      <div className="hidden md:flex md:w-64 md:flex-col">
        <div className="flex flex-col flex-grow bg-white border-r">
          <Sidebar />
        </div>
      </div>

      {/* Mobile Sidebar */}
      <Sheet>
        <SheetTrigger asChild>
          <Button variant="outline" size="icon" className="md:hidden fixed top-4 left-4 z-40 bg-white">
            <Menu className="h-4 w-4" />
          </Button>
        </SheetTrigger>
        <SheetContent side="left" className="p-0 w-64">
          <Sidebar />
        </SheetContent>
      </Sheet>

      {/* Main Content */}
      <div className="flex flex-col flex-1 overflow-hidden">
        <main className="flex-1 overflow-y-auto p-6">{children}</main>
      </div>
    </div>
  )
}