// Updates for partner8-frontend/app/dashboard/scraping/page.tsx

"use client"

import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { Progress } from "@/components/ui/progress"
import { Play, Square, RefreshCw, Activity, Clock, CheckCircle, XCircle, AlertTriangle, Pause, SkipForward } from "lucide-react"
import { apiClient } from "../../../utils/api"

interface ScrapingStatus {
  status: string
  started_at?: string
  completed_at?: string
  records_processed?: number
  error_message?: string
  current_step?: number
  total_steps?: number
  step_name?: string
  progress_percentage?: number
}

interface ScrapingLog {
  id: number
  status: string
  started_by: number
  started_at: string
  completed_at?: string
  error_message?: string
  records_processed: number
}

// Helper functions for cookie management
const getCookie = (name: string): string | null => {
  const value = `; ${document.cookie}`
  const parts = value.split(`; ${name}=`)
  if (parts.length === 2) return parts.pop()?.split(';').shift() || null
  return null
}

const deleteCookie = (name: string) => {
  document.cookie = `${name}=; expires=Thu, 01 Jan 1970 00:00:00 UTC; path=/;`
}

export default function ScrapingPage() {
  const [status, setStatus] = useState<ScrapingStatus>({ status: "idle" })
  const [logs, setLogs] = useState<ScrapingLog[]>([])
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const itemsPerPage = 10
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const [lastUpdateTime, setLastUpdateTime] = useState<Date>(new Date())
  const router = useRouter()

  useEffect(() => {
    const token = getCookie("access_token")
    const role = getCookie("user_role")

    if (!token || role !== "admin") {
      router.push("/dashboard")
      return
    }

    // Initial fetch
    fetchStatus()
    fetchLogs()

    // Set up polling with more aggressive refresh for running status
    const interval = setInterval(() => {
      fetchStatus()
      
      // Also refresh logs if status changed recently
      if (status.status === "completed" || status.status === "failed") {
        fetchLogs()
      }
    }, 2000) // Poll every 2 seconds for more responsive updates

    return () => clearInterval(interval)
  }, [router])

  // Additional effect to handle status changes
  useEffect(() => {
    if (status.status === "completed" || status.status === "failed" || status.status === "stopped") {
      // Refresh logs when scraping completes
      setTimeout(() => fetchLogs(), 1000)
    }
  }, [status.status])

  const clearAllCookies = () => {
    deleteCookie("access_token")
    deleteCookie("username")
    deleteCookie("user_role")
    deleteCookie("user_id")
    deleteCookie("user_email")
  }

  const fetchStatus = async () => {
    try {
      const token = getCookie("access_token")
      if (!token) {
        clearAllCookies()
        router.push("/")
        return
      }

      const data = await apiClient.get("/scraping_status", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })
      
      // Only update if the status actually changed or if it's been more than 5 seconds
      const now = new Date()
      const timeSinceLastUpdate = now.getTime() - lastUpdateTime.getTime()
      
      if (JSON.stringify(data) !== JSON.stringify(status) || timeSinceLastUpdate > 5000) {
        setStatus(data)
        setLastUpdateTime(now)
        console.log("Status updated:", data) // Debug log
      }
      
    } catch (err: any) {
      console.error("Failed to fetch status:", err)
      if (err.message && err.message.includes("401")) {
        clearAllCookies()
        router.push("/")
      } else {
        // Don't show error for status fetch failures unless it's critical
        console.warn("Status fetch failed, will retry:", err.message)
      }
    }
  }

  const fetchLogs = async (page: number = currentPage) => {
    try {
      const token = getCookie("access_token")
      if (!token) {
        clearAllCookies()
        router.push("/")
        return
      }
      const data = await apiClient.get(`/scraping_logs?page=${page}&limit=${itemsPerPage}`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })
      setLogs(data.logs)
      setTotalPages(Math.ceil(data.total / itemsPerPage))
      setCurrentPage(data.page)
    } catch (err: any) {
      console.error("Failed to fetch logs:", err)
      if (err.message && err.message.includes("401")) {
        clearAllCookies()
        router.push("/")
      }
    }
  }

  const startScraping = async () => {
    setIsLoading(true)
    setError("")
    setSuccess("")

    try {
      const token = getCookie("access_token")
      if (!token) {
        clearAllCookies()
        router.push("/")
        return
      }
      await apiClient.post("/start_scraping", null, {
        Authorization: `Bearer ${token}`,
      })

      setSuccess("Scraping started successfully")
      
      // Immediately fetch status to update UI
      setTimeout(() => {
        fetchStatus()
        fetchLogs()
      }, 1000)
      
      setTimeout(() => setSuccess(""), 3000)
    } catch (err: any) {
      console.error("Failed to start scraping:", err)
      if (err.message && err.message.includes("401")) {
        clearAllCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to start scraping")
      }
    } finally {
      setIsLoading(false)
    }
  }

  const stopScraping = async () => {
    setIsLoading(true)
    setError("")
    setSuccess("")

    try {
      const token = getCookie("access_token")
      if (!token) {
        clearAllCookies()
        router.push("/")
        return
      }
      await apiClient.post("/stop_scraping", null, {
        Authorization: `Bearer ${token}`,
      })

      setSuccess("Stop signal sent successfully")
      
      // Immediately fetch status to update UI
      setTimeout(() => {
        fetchStatus()
        fetchLogs()
      }, 1000)
      
      setTimeout(() => setSuccess(""), 3000)
    } catch (err: any) {
      console.error("Failed to stop scraping:", err)
      if (err.message && err.message.includes("401")) {
        clearAllCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to stop scraping")
      }
    } finally {
      setIsLoading(false)
    }
  }

  const getStatusColor = (status: string) => {
    switch (status) {
      case "running":
        return "default"
      case "paused":
        return "secondary"
      case "completed":
        return "default"
      case "failed":
        return "destructive"
      case "stopped":
        return "secondary"
      default:
        return "secondary"
    }
  }

  const getStatusIcon = (status: string) => {
    switch (status) {
      case "running":
        return <Activity className="h-4 w-4 animate-pulse text-green-500" />
      case "paused":
        return <Pause className="h-4 w-4 text-yellow-500" />
      case "completed":
        return <CheckCircle className="h-4 w-4 text-green-500" />
      case "failed":
        return <XCircle className="h-4 w-4 text-red-500" />
      case "stopped":
        return <AlertTriangle className="h-4 w-4 text-orange-500" />
      default:
        return <Square className="h-4 w-4 text-gray-500" />
    }
  }

  const calculateProgress = () => {
    if (status.progress_percentage) {
      return status.progress_percentage
    }
    if (status.current_step && status.total_steps) {
      return (status.current_step / status.total_steps) * 100
    }
    return 0
  }

  const getStepNames = () => {
    const steps = [
      "Download Zillow Data",
      "Merge Zillow Data", 
      "Fetch HUD Data",
      "Fetch NAR Data",
      "Calculate Ratios",
      "Save Final Data"
    ]
    return steps
  }

  const formatDateTime = (dateString: string) => {
    try {
      return new Date(dateString).toLocaleString()
    } catch {
      return dateString
    }
  }

  const isActivelyRunning = status.status === "running"
  const canStart = status.status === "idle" || status.status === "completed" || status.status === "failed" || status.status === "stopped"
  const canStop = status.status === "running" || status.status === "paused"

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="mb-6">
          <h1 className="text-3xl font-bold">Scraping Control</h1>
          <p className="text-gray-600">
            Current Status: <strong>{status.status}</strong> 
            {lastUpdateTime && (
              <span className="text-sm text-gray-500 ml-2">
                (Updated: {lastUpdateTime.toLocaleTimeString()})
              </span>
            )}
          </p>
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

        <div className="grid gap-6 md:grid-cols-2">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                {getStatusIcon(status.status)}
                <span>Current Status</span>
                {isActivelyRunning && (
                  <Badge variant="default" className="animate-pulse ml-2">
                    LIVE
                  </Badge>
                )}
              </CardTitle>
              <CardDescription>Real-time scraping process status with progress tracking</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex items-center space-x-2">
                  <span className="text-sm font-medium">Status:</span>
                  <Badge variant={getStatusColor(status.status)}>
                    {status.status.charAt(0).toUpperCase() + status.status.slice(1)}
                  </Badge>
                  {status.records_processed !== undefined && status.records_processed > 0 && (
                    <Badge variant="outline" className="ml-2">
                      {status.records_processed} records
                    </Badge>
                  )}
                </div>

                {/* Progress Bar - Show for running or recently completed */}
                {(isActivelyRunning || (status.current_step && status.total_steps)) && (
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Progress</span>
                      <span>
                        {status.current_step || 0}/{status.total_steps || 6} steps 
                        ({Math.round(calculateProgress())}%)
                      </span>
                    </div>
                    <Progress value={calculateProgress()} className="w-full" />
                    {status.step_name && (
                      <p className="text-sm text-gray-600">
                        <strong>Current:</strong> {status.step_name}
                      </p>
                    )}
                  </div>
                )}

                {status.started_at && (
                  <div>
                    <span className="text-sm font-medium">Started:</span>
                    <p className="text-sm text-gray-600 mt-1">{formatDateTime(status.started_at)}</p>
                  </div>
                )}

                {status.completed_at && (
                  <div>
                    <span className="text-sm font-medium">Completed:</span>
                    <p className="text-sm text-gray-600 mt-1">{formatDateTime(status.completed_at)}</p>
                  </div>
                )}

                {status.error_message && (
                  <div>
                    <span className="text-sm font-medium text-red-600">Error:</span>
                    <p className="text-sm text-red-600 mt-1 bg-red-50 p-2 rounded">
                      {status.error_message}
                    </p>
                  </div>
                )}

                {/* Pipeline Steps Overview */}
                {isActivelyRunning && status.current_step && (
                  <div className="mt-4">
                    <span className="text-sm font-medium">Pipeline Steps:</span>
                    <div className="mt-2 space-y-1">
                      {getStepNames().map((stepName, index) => (
                        <div key={index} className={`flex items-center space-x-2 text-xs p-2 rounded ${
                          status.current_step && index + 1 === status.current_step 
                            ? 'bg-blue-50 border border-blue-200' 
                            : status.current_step && index + 1 < status.current_step
                            ? 'bg-green-50 border border-green-200'
                            : 'bg-gray-50'
                        }`}>
                          <span className={`w-5 h-5 rounded-full flex items-center justify-center text-xs ${
                            status.current_step && index + 1 === status.current_step
                              ? 'bg-blue-500 text-white'
                              : status.current_step && index + 1 < status.current_step
                              ? 'bg-green-500 text-white'
                              : 'bg-gray-300 text-gray-600'
                          }`}>
                            {index + 1 < (status.current_step || 0) ? '✓' : index + 1}
                          </span>
                          <span className={
                            status.current_step && index + 1 === status.current_step 
                              ? 'font-medium text-blue-700' 
                              : status.current_step && index + 1 < status.current_step
                              ? 'text-green-700'
                              : 'text-gray-600'
                          }>
                            {stepName}
                          </span>
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </CardContent>
          </Card>

          <Card>
            <CardHeader>
              <CardTitle>Enhanced Controls</CardTitle>
              <CardDescription>Start, stop, or monitor the scraping process</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <Button
                  onClick={startScraping}
                  disabled={isLoading || !canStart}
                  className="w-full"
                  variant={canStart ? "default" : "secondary"}
                >
                  <Play className="h-4 w-4 mr-2" />
                  {canStart ? "Start Scraping" : `Pipeline ${status.status}`}
                </Button>

                <Button
                  onClick={stopScraping}
                  disabled={isLoading || !canStop}
                  variant="destructive"
                  className="w-full"
                >
                  <Square className="h-4 w-4 mr-2" />
                  Stop Pipeline
                </Button>

                <Button
                  onClick={() => {
                    fetchStatus()
                    fetchLogs()
                  }}
                  variant="outline"
                  className="w-full bg-transparent"
                  disabled={isLoading}
                >
                  <RefreshCw className={`h-4 w-4 mr-2 ${isLoading ? 'animate-spin' : ''}`} />
                  Refresh Status
                </Button>
              </div>

              {/* Quick Status Info */}
              <div className="mt-4 p-3 bg-gray-50 rounded-lg">
                <div className="text-sm space-y-1">
                  <div className="flex justify-between">
                    <span>Last Update:</span>
                    <span className="font-mono text-xs">
                      {lastUpdateTime.toLocaleTimeString()}
                    </span>
                  </div>
                  {isActivelyRunning && status.records_processed && (
                    <div className="flex justify-between">
                      <span>Records Processed:</span>
                      <span className="font-medium">
                        {status.records_processed.toLocaleString()}
                      </span>
                    </div>
                  )}
                </div>
              </div>
            </CardContent>
          </Card>
        </div>

        {/* Rest of the component remains the same... */}
        <Card>
          <CardHeader>
            <CardTitle>Scraping History</CardTitle>
            <CardDescription>Recent scraping operations and their results</CardDescription>
          </CardHeader>
          <CardContent>
            {logs.length > 0 ? (
              <>
                <Table>
                  <TableHeader>
                    <TableRow>
                      <TableHead>Status</TableHead>
                      <TableHead>Started</TableHead>
                      <TableHead>Completed</TableHead>
                      <TableHead>Records</TableHead>
                      <TableHead>Duration</TableHead>
                      <TableHead>Details</TableHead>
                    </TableRow>
                  </TableHeader>
                  <TableBody>
                    {logs.map((log) => (
                      <TableRow key={log.id}>
                        <TableCell>
                          <div className="flex items-center space-x-2">
                            {getStatusIcon(log.status)}
                            <Badge variant={getStatusColor(log.status)}>
                              {log.status.charAt(0).toUpperCase() + log.status.slice(1)}
                            </Badge>
                          </div>
                        </TableCell>
                        <TableCell>{formatDateTime(log.started_at)}</TableCell>
                        <TableCell>{log.completed_at ? formatDateTime(log.completed_at) : "-"}</TableCell>
                        <TableCell>{log.records_processed ? log.records_processed.toLocaleString() : "0"}</TableCell>
                        <TableCell>
                          {log.completed_at
                            ? `${Math.round(
                              (new Date(log.completed_at).getTime() - new Date(log.started_at).getTime()) / 1000
                            )}s`
                            : "-"}
                        </TableCell>
                        <TableCell>
                          {log.error_message && (
                            <span className="text-xs text-red-600 truncate max-w-xs" title={log.error_message}>
                              {log.error_message.length > 50
                                ? `${log.error_message.substring(0, 50)}...`
                                : log.error_message}
                            </span>
                          )}
                        </TableCell>
                      </TableRow>
                    ))}
                  </TableBody>
                </Table>
                <div className="flex justify-between items-center mt-4">
                  <Button
                    onClick={() => fetchLogs(currentPage - 1)}
                    disabled={currentPage === 1 || isLoading}
                    variant="outline"
                    size="sm"
                  >
                    Previous
                  </Button>
                  <span className="text-sm text-gray-600">
                    Page {currentPage} of {totalPages}
                  </span>
                  <Button
                    onClick={() => fetchLogs(currentPage + 1)}
                    disabled={currentPage === totalPages || isLoading}
                    variant="outline"
                    size="sm"
                  >
                    Next
                  </Button>
                </div>
              </>
            ) : (
              <div className="text-center py-8 text-gray-500">
                <Clock className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No scraping history available</p>
                <p className="text-sm mt-2">Start your first scraping operation to see logs here</p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Live Activity Monitor for Running Status */}
        {isActivelyRunning && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Activity className="h-5 w-5 animate-pulse text-green-500" />
                <span>Live Activity Monitor</span>
                <Badge variant="default" className="animate-pulse">
                  ACTIVE
                </Badge>
              </CardTitle>
              <CardDescription>Real-time pipeline activity and progress</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="grid grid-cols-2 gap-4">
                  <div className="bg-blue-50 p-3 rounded-lg">
                    <div className="text-sm font-medium text-blue-800">Current Step</div>
                    <div className="text-lg font-bold text-blue-600">
                      {status.current_step || 0} / {status.total_steps || 6}
                    </div>
                  </div>
                  <div className="bg-green-50 p-3 rounded-lg">
                    <div className="text-sm font-medium text-green-800">Progress</div>
                    <div className="text-lg font-bold text-green-600">
                      {Math.round(calculateProgress())}%
                    </div>
                  </div>
                </div>

                {status.records_processed && status.records_processed > 0 && (
                  <div className="bg-yellow-50 p-3 rounded-lg">
                    <div className="text-sm font-medium text-yellow-800">Records Processed</div>
                    <div className="text-lg font-bold text-yellow-600">
                      {status.records_processed.toLocaleString()}
                    </div>
                  </div>
                )}

                <div className="bg-gray-50 p-4 rounded-lg">
                  <div className="text-sm font-medium mb-2">Quick Actions</div>
                  <div className="flex space-x-2">
                    <Button size="sm" variant="outline" onClick={stopScraping} disabled={isLoading}>
                      <Square className="h-3 w-3 mr-1" />
                      Stop
                    </Button>
                    <Button size="sm" variant="outline" onClick={fetchStatus} disabled={isLoading}>
                      <RefreshCw className="h-3 w-3 mr-1" />
                      Refresh
                    </Button>
                  </div>
                </div>
              </div>
            </CardContent>
          </Card>
        )}
      </div>
    </DashboardLayout>
  )
}