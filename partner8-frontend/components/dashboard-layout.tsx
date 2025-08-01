"use client"

import type React from "react"
import { useState, useEffect } from "react"
import { usePathname, useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Sheet, SheetContent, SheetTrigger } from "@/components/ui/sheet"
import { Home, Users, MessageSquare, LogOut, Menu, Play, Trash2 } from "lucide-react"

interface DashboardLayoutProps {
  children: React.ReactNode
}

import { getCookie, clearAuthCookies } from "../utils/cookies";

export default function DashboardLayout({ children }: DashboardLayoutProps) {
  const [userRole, setUserRole] = useState("")
  const [username, setUsername] = useState("")
  const [isLoading, setIsLoading] = useState(true)
  const pathname = usePathname()
  const router = useRouter()

  useEffect(() => {
    const initializeDashboard = () => {
      ("DashboardLayout: Initializing dashboard...")
      
      // Get user data from cookies (middleware already verified the token)
      const usernameFromCookie = getCookie("username")
      const roleFromCookie = getCookie("user_role")
      
      if (!usernameFromCookie || !roleFromCookie) {
        clearAuthCookies()
        router.replace("/")
        return
      }

      setUserRole(roleFromCookie)
      setUsername(usernameFromCookie)
      
      // Handle role-based redirection
      if (roleFromCookie === 'user') {
        if (pathname === '/dashboard/scraping' || pathname === '/dashboard/users') {
          router.replace("/dashboard/chat")
          return
        }
      }
      
      setIsLoading(false)
    }

    initializeDashboard()
  }, [router, pathname])

  

  const handleLogout = () => {
    try {
      clearAuthCookies()
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
          <p className="mt-4 text-gray-600">Loading dashboard...</p>
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