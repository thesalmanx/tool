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

interface ScrapingStatus {
  status: string
  started_at?: string
  completed_at?: string
  records_processed?: number
  error_message?: string
  current_step?: number
  total_steps?: number
  step_name?: string
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
  const itemsPerPage = 10 // 10 entries at a time
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const [isPaused, setIsPaused] = useState(false)
  const router = useRouter()

  useEffect(() => {
    const token = getCookie("access_token")
    const role = getCookie("user_role")

    if (!token || role !== "admin") {
      router.push("/dashboard")
      return
    }

    fetchStatus()
    fetchLogs()

    // Poll status every 3 seconds when running
    const interval = setInterval(() => {
      fetchStatus()
      if (status.status === "completed" || status.status === "failed") {
        fetchLogs() // Refresh logs when status changes
      }
    }, 3000)

    return () => clearInterval(interval)
  }, [router, status.status])

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
      const response = await fetch("http://localhost:8000/scraping_status", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })
      if (response.ok) {
        const data = await response.json()
        setStatus(data)
        setIsPaused(data.status === "paused")
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      }
    } catch (err) {
      console.error("Failed to fetch status:", err)
    }
  }

  const fetchLogs = async (page: number = currentPage) => {
    try {
      const token = getCookie("access_token")
      const response = await fetch(`http://localhost:8000/scraping_logs?page=${page}&limit=${itemsPerPage}`, {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })
      if (response.ok) {
        const data = await response.json()
        setLogs(data.logs)
        setTotalPages(Math.ceil(data.total / itemsPerPage))
        setCurrentPage(data.page)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      }
    } catch (err) {
      console.error("Failed to fetch logs:", err)
    }
  }

  const startScraping = async () => {
    setIsLoading(true)
    setError("")
    setSuccess("")

    try {
      const token = getCookie("access_token")
      const response = await fetch("http://localhost:8000/start_scraping", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        setSuccess("Scraping started successfully")
        fetchStatus()
        fetchLogs()
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        const errorData = await response.json()
        setError(errorData.detail || "Failed to start scraping")
      }
    } catch (err) {
      setError("Network error")
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
      const response = await fetch("http://localhost:8000/stop_scraping", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        setSuccess("Stop signal sent successfully")
        fetchStatus()
        fetchLogs()
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        const errorData = await response.json()
        setError(errorData.detail || "Failed to stop scraping")
      }
    } catch (err) {
      setError("Network error")
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

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="mb-6">
          <h1 className="text-3xl font-bold">Scraping Control</h1>
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
                </div>

                {/* Progress Bar */}
                {status.status === "running" && status.current_step && status.total_steps && (
                  <div className="space-y-2">
                    <div className="flex justify-between text-sm">
                      <span>Progress</span>
                      <span>{status.current_step}/{status.total_steps} steps</span>
                    </div>
                    <Progress value={calculateProgress()} className="w-full" />
                    {status.step_name && (
                      <p className="text-sm text-gray-600">Current: {status.step_name}</p>
                    )}
                  </div>
                )}

                {status.started_at && (
                  <div>
                    <span className="text-sm font-medium">Started:</span>
                    <p className="text-sm text-gray-600 mt-1">{new Date(status.started_at).toLocaleString()}</p>
                  </div>
                )}

                {status.completed_at && (
                  <div>
                    <span className="text-sm font-medium">Completed:</span>
                    <p className="text-sm text-gray-600 mt-1">{new Date(status.completed_at).toLocaleString()}</p>
                  </div>
                )}

                {status.records_processed !== undefined && status.records_processed > 0 && (
                  <div>
                    <span className="text-sm font-medium">Records Processed:</span>
                    <p className="text-sm text-gray-600 mt-1">{status.records_processed.toLocaleString()}</p>
                  </div>
                )}

                {status.error_message && (
                  <div>
                    <span className="text-sm font-medium text-red-600">Error:</span>
                    <p className="text-sm text-red-600 mt-1">{status.error_message}</p>
                  </div>
                )}

                {/* Pipeline Steps Overview */}
                {(status.status === "running" || status.status === "paused") && (
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
              <CardDescription>Start, stop, pause, or resume the scraping process</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-3">
                <Button
                  onClick={startScraping}
                  disabled={isLoading || status.status === "running" || status.status === "paused"}
                  className="w-full"
                  variant={status.status === "running" ? "secondary" : "default"}
                >
                  <Play className="h-4 w-4 mr-2" />
                  {status.status === "running" || status.status === "paused" ? "Pipeline Active" : "Start Scraping"}
                </Button>

               

                

                <Button
                  onClick={stopScraping}
                  disabled={isLoading || (status.status !== "running" && status.status !== "paused")}
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
                  <RefreshCw className="h-4 w-4 mr-2" />
                  Refresh Status
                </Button>
              </div>

             
            </CardContent>
          </Card>
        </div>

        <Card>
          <CardHeader>
            <CardTitle>Scraping History</CardTitle>
            <CardDescription>Recent scraping operations and their results</CardDescription>
          </CardHeader>
          <CardContent>
            {logs.length > 0 ? (
              <><Table>
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
                      <TableCell>{new Date(log.started_at).toLocaleString()}</TableCell>
                      <TableCell>{log.completed_at ? new Date(log.completed_at).toLocaleString() : "-"}</TableCell>
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
              </Table><div className="flex justify-between items-center mt-4">
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
                </div></>
            ) : (
              <div className="text-center py-8 text-gray-500">
                <Clock className="h-12 w-12 mx-auto mb-4 opacity-50" />
                <p>No scraping history available</p>
                <p className="text-sm mt-2">Start your first scraping operation to see logs here</p>
              </div>
            )}
          </CardContent>
        </Card>

        {/* Real-time Activity Monitor */}
        {status.status === "running" && (
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center space-x-2">
                <Activity className="h-5 w-5 animate-pulse text-green-500" />
                <span>Live Activity Monitor</span>
              </CardTitle>
              <CardDescription>Real-time pipeline activity and progress</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                <div className="flex items-center justify-between">
                  <span className="text-sm font-medium">Pipeline Status</span>
                  <Badge variant="default" className="animate-pulse">
                    <Activity className="h-3 w-3 mr-1" />
                    Active
                  </Badge>
                </div>
                
                {status.current_step && status.total_steps && (
                  <div className="grid grid-cols-2 gap-4">
                    <div className="bg-blue-50 p-3 rounded-lg">
                      <div className="text-sm font-medium text-blue-800">Current Step</div>
                      <div className="text-lg font-bold text-blue-600">
                        {status.current_step} / {status.total_steps}
                      </div>
                    </div>
                    <div className="bg-green-50 p-3 rounded-lg">
                      <div className="text-sm font-medium text-green-800">Progress</div>
                      <div className="text-lg font-bold text-green-600">
                        {Math.round(calculateProgress())}%
                      </div>
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