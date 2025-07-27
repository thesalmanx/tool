"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import LoginForm from "@/components/login-form"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"

export default function HomePage() {
  const [showLogin, setShowLogin] = useState(false)
  const router = useRouter()

  useEffect(() => {
    const token = localStorage.getItem("access_token")

    if (token) {
      // If token exists, redirect to dashboard immediately
      router.replace("/dashboard")
    } else {
      // If no token, show the login form
      setShowLogin(true)
    }
  }, [router])

  if (!showLogin) {
    // Render a loading spinner or null while checking auth and redirecting
    return (
      <div className="min-h-screen flex items-center justify-center bg-gray-50">
        <div className="text-center">
          <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
          <p className="mt-4 text-gray-600">Checking authentication...</p>
        </div>
      </div>
    )
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-gray-50 py-12 px-4 sm:px-6 lg:px-8">
      <div className="max-w-md w-full space-y-8">
        <div className="text-center">
          <h1 className="text-3xl font-bold text-gray-900">Partners8</h1>
          <p className="mt-2 text-gray-600">Welcome to the Partners8 platform</p>
        </div>
        <Card>
          <CardHeader>
            <CardTitle>Access Your Account</CardTitle>
            <CardDescription>Sign in to your existing account or create a new one</CardDescription>
          </CardHeader>
          <CardContent>
            <LoginForm />
          </CardContent>
        </Card>
      </div>
    </div>
  )
}
