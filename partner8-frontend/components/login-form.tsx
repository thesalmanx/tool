"use client"

import React, { useState } from "react"
import { useRouter } from "next/navigation"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"

// For production, you'll need to define your API base URL
const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "0.0.0.0:8000"

import { setCookie, clearAuthCookies } from "../utils/cookies";

export default function LoginForm() {
  const [loginData, setLoginData] = useState({ username: "", password: "" })
  const [signupData, setSignupData] = useState({ username: "", email: "", password: "" })
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const router = useRouter()

  const handleLogin = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    setError("")
    setSuccess("")

    try {
      // Clear any existing auth data first
      clearAuthCookies()

      // Use URLSearchParams for proper form encoding
      const formData = new URLSearchParams()
      formData.append("username", loginData.username.trim())
      formData.append("password", loginData.password)

      console.log(API_BASE_URL)
      const response = await fetch(`${API_BASE_URL}/token`, {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
        },
        body: formData.toString(),
      })


      if (response.ok) {
        const data = await response.json()

        if (!data.access_token || !data.user) {
          throw new Error('Invalid response format from server')
        }

        // Store authentication data
        const tokenValue = data.access_token
        const userInfo = data.user

        // Set cookies with better error handling
        try {
          setCookie("access_token", tokenValue, 7)
          setCookie("username", userInfo.username, 7)
          setCookie("user_role", userInfo.role, 7)
          setCookie("user_id", userInfo.id.toString(), 7)
          setCookie("user_email", userInfo.email, 7)
        } catch (cookieError) {
          console.error('Error setting cookies:', cookieError)
          throw new Error('Failed to save authentication data')
        }

        setSuccess("Login successful! Redirecting...")
        
        // Clear the form
        setLoginData({ username: "", password: "" })
        
        // Small delay to show success message, then redirect
        setTimeout(() => {
          router.push("/dashboard")
        }, 1000)

      } else {
        const errorText = await response.text()
        let errorData
        try {
          errorData = JSON.parse(errorText)
        } catch {
          errorData = { detail: errorText || 'Unknown error occurred' }
        }
        console.error('Login failed:', errorData)
        setError(errorData.detail || `Login failed (${response.status})`)
      }
    } catch (err: any) {
      console.error("Login error:", err)
      if (err instanceof TypeError && err.message.includes('fetch')) {
        setError("Network error. Please check your connection and try again.")
      } else {
        setError(err.message || "An unexpected error occurred. Please try again.")
      }
    } finally {
      setIsLoading(false)
    }
  }

  const handleSignup = async (e: React.FormEvent) => {
    e.preventDefault()
    setIsLoading(true)
    setError("")
    setSuccess("")

    // Basic validation
    if (!signupData.username.trim() || !signupData.email.trim() || !signupData.password) {
      setError("All fields are required")
      setIsLoading(false)
      return
    }

    if (signupData.password.length < 6) {
      setError("Password must be at least 6 characters")
      setIsLoading(false)
      return
    }

    // Basic email validation
    const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/
    if (!emailRegex.test(signupData.email.trim())) {
      setError("Please enter a valid email address")
      setIsLoading(false)
      return
    }

    try {

      const response = await fetch(`${API_BASE_URL}/signup`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          username: signupData.username.trim(),
          email: signupData.email.trim(),
          password: signupData.password
        }),
      })


      if (response.ok) {
        const data = await response.json()
        setSuccess(data.message || "Account created successfully! Please wait for admin approval.")
        setSignupData({ username: "", email: "", password: "" })
      } else {
        const errorText = await response.text()
        let errorData
        try {
          errorData = JSON.parse(errorText)
        } catch {
          errorData = { detail: errorText || 'Unknown error occurred' }
        }
        console.error('Signup failed:', errorData)
        setError(errorData.detail || `Signup failed (${response.status})`)
      }
    } catch (err: any) {
      console.error("Signup error:", err)
      if (err instanceof TypeError && err.message.includes('fetch')) {
        setError("Network error. Please check your connection and try again.")
      } else {
        setError("Network error. Please try again.")
      }
    } finally {
      setIsLoading(false)
    }
  }

  const handleLoginSubmit = (e: React.MouseEvent) => {
    e.preventDefault()
    handleLogin(e as any)
  }

  const handleSignupSubmit = (e: React.MouseEvent) => {
    e.preventDefault()
    handleSignup(e as any)
  }

  return (
    <div className="w-full max-w-md mx-auto">
      <Tabs defaultValue="login" className="w-full">
        <TabsList className="grid w-full grid-cols-2">
          <TabsTrigger value="login">Login</TabsTrigger>
          <TabsTrigger value="signup">Sign Up</TabsTrigger>
        </TabsList>

        <TabsContent value="login">
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="username">Username</Label>
              <Input
                id="username"
                type="text"
                value={loginData.username}
                onChange={(e) => setLoginData({ ...loginData, username: e.target.value })}
                required
                disabled={isLoading}
                autoComplete="username"
                placeholder="Enter your username"
                onKeyDown={(e) => e.key === 'Enter' && !isLoading && handleLogin(e as any)}
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="password">Password</Label>
              <Input
                id="password"
                type="password"
                value={loginData.password}
                onChange={(e) => setLoginData({ ...loginData, password: e.target.value })}
                required
                disabled={isLoading}
                autoComplete="current-password"
                placeholder="Enter your password"
                onKeyDown={(e) => e.key === 'Enter' && !isLoading && handleLogin(e as any)}
              />
            </div>
            {error && (
              <Alert variant="destructive">
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
            {success && (
              <Alert className="border-green-200 bg-green-50">
                <AlertDescription className="text-green-800">{success}</AlertDescription>
              </Alert>
            )}
            <Button onClick={handleLoginSubmit} className="w-full" disabled={isLoading}>
              {isLoading ? "Signing in..." : "Sign In"}
            </Button>
          </div>
        </TabsContent>

        <TabsContent value="signup">
          <div className="space-y-4">
            <div className="space-y-2">
              <Label htmlFor="signup-username">Username</Label>
              <Input
                id="signup-username"
                type="text"
                value={signupData.username}
                onChange={(e) => setSignupData({ ...signupData, username: e.target.value })}
                required
                disabled={isLoading}
                autoComplete="username"
                placeholder="Choose a username"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="signup-email">Email</Label>
              <Input
                id="signup-email"
                type="email"
                value={signupData.email}
                onChange={(e) => setSignupData({ ...signupData, email: e.target.value })}
                required
                disabled={isLoading}
                autoComplete="email"
                placeholder="Enter your email"
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="signup-password">Password</Label>
              <Input
                id="signup-password"
                type="password"
                value={signupData.password}
                onChange={(e) => setSignupData({ ...signupData, password: e.target.value })}
                required
                disabled={isLoading}
                autoComplete="new-password"
                minLength={6}
                placeholder="Create a password (min 6 characters)"
              />
            </div>
            {error && (
              <Alert variant="destructive">
                <AlertDescription>{error}</AlertDescription>
              </Alert>
            )}
            {success && (
              <Alert className="border-green-200 bg-green-50">
                <AlertDescription className="text-green-800">{success}</AlertDescription>
              </Alert>
            )}
            <Button onClick={handleSignupSubmit} className="w-full" disabled={isLoading}>
              {isLoading ? "Creating account..." : "Sign Up"}
            </Button>
            <div className="text-sm text-gray-600 text-center">
              <p>New accounts require admin approval before you can sign in.</p>
            </div>
          </div>
        </TabsContent>
      </Tabs>
    </div>
  )
}