"use client"

import type React from "react"
import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Tabs, TabsContent } from "@/components/ui/tabs"
import { Label } from "@/components/ui/label"
import { Shield } from "lucide-react"


interface UserInfo {
  id: string
  username: string
  email: string
  role: string
}

// Helper function for cookie management with proper decoding
const getCookie = (name: string): string | null => {
  if (typeof document === 'undefined') return null;
  try {
    const value = `; ${document.cookie}`
    const parts = value.split(`; ${name}=`)
    if (parts.length === 2) {
      const cookieValue = parts.pop()?.split(';').shift() || ''
      // Properly decode the cookie value
      return decodeURIComponent(cookieValue)
    }
    return null
  } catch (error) {
    console.error('Error getting cookie:', error)
    return null
  }
}

const deleteCookie = (name: string) => {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`
}

export default function DashboardPage() {
  const [userInfo, setUserInfo] = useState<UserInfo | null>(null)
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const router = useRouter()

  useEffect(() => {
    const initializeDashboard = async () => {
      // Since middleware already verified the token, just get user data from cookies
      const username = getCookie("username")
      const email = getCookie("user_email")
      const role = getCookie("user_role")
      const id = getCookie("user_id")


      if (!username || !email || !role || !id) {
        // If cookies are missing, redirect to home
        router.push("/")
        return
      }

      // Set user info from cookies (properly decoded)
      setUserInfo({
        id,
        username,
        email, // This should now show @ instead of %40
        role
      })
      
      setIsLoading(false)
    }

    initializeDashboard()
  }, [router])

  // Optional: Keep this function if you want to refresh user data
  const fetchUserInfo = async () => {
    try {
      const token = getCookie("access_token")
      if (!token) {
        router.push("/")
        return
      }

      const response = await fetch("http://localhost:8000/verify-token", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        const data = await response.json()
        setUserInfo(data.user)
      } else if (response.status === 401) {
        // Clear cookies and redirect
        deleteCookie("access_token")
        deleteCookie("username")
        deleteCookie("user_role")
        deleteCookie("user_id")
        deleteCookie("user_email")
        router.push("/")
      } else {
        setError("Failed to fetch user information")
      }
    } catch (err) {
      setError("Network error")
    } finally {
      setIsLoading(false)
    }
  }

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
            <p className="mt-4 text-gray-600">Loading dashboard...</p>
          </div>
        </div>
      </DashboardLayout>
    )
  }

  if (!userInfo) {
    return null
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold">Welcome back, {userInfo.username}!</h1>
            <p className="text-gray-600">Manage your Partners8 system from this dashboard</p>
          </div>
          <div className="flex items-center space-x-2">
            <Badge variant={userInfo.role === 'admin' ? 'default' : 'secondary'}>
              <Shield className="h-3 w-3 mr-1" />
              {userInfo.role}
            </Badge>
          </div>
        </div>

        {error && (
          <Alert variant="destructive">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        {success && (
          <Alert>
            <AlertDescription>{success}</AlertDescription>
          </Alert>
        )}

        <Tabs defaultValue="overview" className="space-y-6">
          <TabsContent value="overview" className="space-y-6">
            {/* Account Information */}
            <Card>
              <CardHeader>
                <CardTitle>Your Info</CardTitle>
                <CardDescription>Details of the currently logged-in user</CardDescription>
              </CardHeader>
              <CardContent>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <Label className="text-sm font-medium text-gray-700">User ID</Label>
                    <p className="text-sm text-gray-900">{userInfo.id}</p>
                  </div>
                  <div>
                    <Label className="text-sm font-medium text-gray-700">Username</Label>
                    <p className="text-sm text-gray-900">{userInfo.username}</p>
                  </div>
                  <div>
                    <Label className="text-sm font-medium text-gray-700">Email</Label>
                    <p className="text-sm text-gray-900">{userInfo.email}</p>
                  </div>
                  <div>
                    <Label className="text-sm font-medium text-gray-700">Role</Label>
                    <p className="text-sm text-gray-900 capitalize">{userInfo.role}</p>
                  </div>
                </div>
              </CardContent>
            </Card>
          </TabsContent>
        </Tabs>
      </div>
    </DashboardLayout>
  )
}