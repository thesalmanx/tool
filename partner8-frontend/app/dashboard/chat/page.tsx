"use client"

import type React from "react"
import { useEffect, useState, useRef } from "react"
import { useRouter } from "next/navigation"
import * as XLSX from 'xlsx'
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { ScrollArea } from "@/components/ui/scroll-area"
import { Alert, AlertDescription } from "@/components/ui/alert"
import { Badge } from "@/components/ui/badge"
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs"
import { Send, Bot, User, Database, Search, ExternalLink, BarChart3, Download, FileText, Table, FileSpreadsheet } from "lucide-react"

interface Message {
  id: string
  content: string
  sender: "user" | "bot"
  timestamp: Date
  isGrounded?: boolean
  sources?: Array<{ title: string; uri: string }>
  sessionId?: string
  queryType?: string
  sqlQuery?: string
  queryResults?: any[]
  summary?: string
}

interface ChatResponse {
  response: string
  session_id: string
  is_grounded: boolean
  sources?: Array<{ title: string; uri: string }>
  query_type: string
  sql_query?: string
  query_results?: any[]
}

interface DatabaseInfo {
  available: boolean
  total_rows?: number
  columns?: Array<{ name: string; type: string }>
  sample_queries?: string[]
  message?: string
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

export default function ChatPage() {
  const [messages, setMessages] = useState<Message[]>([])
  const [inputMessage, setInputMessage] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null)
  const [activeTab, setActiveTab] = useState("chat")
  const [databaseInfo, setDatabaseInfo] = useState<DatabaseInfo | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const router = useRouter()

  useEffect(() => {
    const token = getCookie("access_token")
    if (!token) {
      router.push("/")
      return
    }

    loadDatabaseInfo()

    setMessages([
      {
        id: "welcome",
        content: "Hello! I'm your Partners8 AI assistant. I can help you with both general questions and real estate data analysis. Try asking me about property data, or just chat with me about anything!",
        sender: "bot",
        timestamp: new Date(),
      },
    ])
  }, [router])

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }

  const clearAllCookies = () => {
    deleteCookie("access_token")
    deleteCookie("username")
    deleteCookie("user_role")
    deleteCookie("user_id")
    deleteCookie("user_email")
  }

  const loadDatabaseInfo = async () => {
    try {
      const token = getCookie("access_token")
      const response = await fetch("http://localhost:8000/database/info", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        const data = await response.json()
        setDatabaseInfo(data)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      }
    } catch (err) {
      console.error("Failed to load database info:", err)
    }
  }

  const sendChatMessage = async (e: React.FormEvent | React.MouseEvent) => {
    e.preventDefault()
    if (!inputMessage.trim() || isLoading) return

    const userMessage: Message = {
      id: Date.now().toString(),
      content: inputMessage,
      sender: "user",
      timestamp: new Date(),
    }

    setMessages((prev) => [...prev, userMessage])
    const messageToSend = inputMessage
    setInputMessage("")
    setIsLoading(true)
    setError("")

    try {
      const token = getCookie("access_token")
      const response = await fetch("http://localhost:8000/chat", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({
          message: messageToSend,
          session_id: currentSessionId,
        }),
      })

      if (response.ok) {
        const data: ChatResponse = await response.json()

        if (!currentSessionId) {
          setCurrentSessionId(data.session_id)
        }

        // Extract summary from response for data queries
        let summary = ""
        let cleanResponse = data.response
        
        if (data.query_type === "data_query" && data.response.includes("**Data Analysis Results:**")) {
          const parts = data.response.split("**Data Analysis Results:**")
          if (parts.length > 1) {
            const summaryPart = parts[1].split("**Found")[0].trim()
            summary = summaryPart.replace(/\n\n/g, " ").trim()
            cleanResponse = summary
          }
        }

        const botMessage: Message = {
          id: (Date.now() + 1).toString(),
          content: cleanResponse,
          sender: "bot",
          timestamp: new Date(),
          isGrounded: data.is_grounded,
          sources: data.sources,
          sessionId: data.session_id,
          queryType: data.query_type,
          sqlQuery: data.sql_query,
          queryResults: data.query_results,
          summary: summary || cleanResponse,
        }
        setMessages((prev) => [...prev, botMessage])
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        const errorData = await response.json()
        setError(errorData.detail || "Failed to send message")
      }
    } catch (err) {
      setError("Network error. Please try again.")
    } finally {
      setIsLoading(false)
    }
  }

  const useSampleQuery = (query: string) => {
    setInputMessage(query)
    setActiveTab("chat")
  }

  const downloadResults = (data: any[], filename: string, format: 'csv' | 'excel' = 'csv') => {
    if (!data || data.length === 0) return

    const headers = Object.keys(data[0])

    if (format === 'csv') {
      const csvContent = [
        headers.join(","),
        ...data.map(row => 
          headers.map(header => {
            const value = row[header]
            if (typeof value === 'string' && (value.includes(',') || value.includes('"'))) {
              return `"${value.replace(/"/g, '""')}"`
            }
            return value || ''
          }).join(",")
        )
      ].join("\n")

      const blob = new Blob([csvContent], { type: "text/csv" })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `${filename}.csv`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    } else if (format === 'excel') {
      // Create workbook and worksheet
      const workbook = {
        SheetNames: ['Data'],
        Sheets: {
          'Data': {}
        }
      }

      // Add headers
      headers.forEach((header, colIndex) => {
        const cellRef = String.fromCharCode(65 + colIndex) + '1'
        workbook.Sheets['Data'][cellRef] = {
          v: header,
          t: 's',
          s: {
            font: { bold: true },
            fill: { fgColor: { rgb: 'E5E5E5' } }
          }
        }
      })

      // Add data rows
      data.forEach((row, rowIndex) => {
        headers.forEach((header, colIndex) => {
          const cellRef = String.fromCharCode(65 + colIndex) + (rowIndex + 2)
          const value = row[header]
          
          if (typeof value === 'number') {
            workbook.Sheets['Data'][cellRef] = { v: value, t: 'n' }
          } else if (value instanceof Date) {
            workbook.Sheets['Data'][cellRef] = { v: value, t: 'd' }
          } else {
            workbook.Sheets['Data'][cellRef] = { v: value || '', t: 's' }
          }
        })
      })

      // Set worksheet range
      const range = `A1:${String.fromCharCode(64 + headers.length)}${data.length + 1}`
      workbook.Sheets['Data']['!ref'] = range

      // Auto-width columns
      const colWidths = headers.map((header, colIndex) => {
        const headerWidth = header.length
        const maxDataWidth = Math.max(...data.map(row => 
          String(row[header] || '').length
        ))
        return { wch: Math.max(headerWidth, maxDataWidth, 10) }
      })
      workbook.Sheets['Data']['!cols'] = colWidths

      // Convert to binary string
      const wbout = XLSX.write(workbook, { bookType: 'xlsx', type: 'binary' })
      
      // Convert to blob
      const s2ab = (s: string) => {
        const buf = new ArrayBuffer(s.length)
        const view = new Uint8Array(buf)
        for (let i = 0; i < s.length; i++) view[i] = s.charCodeAt(i) & 0xFF
        return buf
      }

      const blob = new Blob([s2ab(wbout)], { type: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement("a")
      a.href = url
      a.download = `${filename}.xlsx`
      document.body.appendChild(a)
      a.click()
      document.body.removeChild(a)
      URL.revokeObjectURL(url)
    }
  }

  const formatTime = (date: Date) => {
    return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
  }

  const renderMarkdown = (text: string) => {
    // Simple markdown rendering for bold text and basic formatting
    return text
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/\n/g, '<br />')
  }

  const renderDataQueryResult = (message: Message) => {
    if (message.queryType !== "data_query" || !message.queryResults) return null

    const results = message.queryResults
    if (results.length === 0) {
      return (
        <div className="mt-4 p-4 bg-gray-50 rounded-lg w-full overflow-hidden">
          <div className="flex items-center space-x-2 mb-2">
            <Database className="h-4 w-4 text-blue-600 flex-shrink-0" />
            <Badge variant="outline">No Data Found</Badge>
          </div>
          <p className="text-sm text-gray-600">No records match your query criteria.</p>
        </div>
      )
    }

    const headers = Object.keys(results[0])

    return (
      <div className="mt-4 space-y-4 w-full overflow-hidden">
        {/* Data Table Section */}
        <div className="bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="border-b border-gray-200 p-4">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center space-x-2 flex-wrap">
                <Table className="h-4 w-4 text-green-600 flex-shrink-0" />
                <Badge variant="secondary">Query Results</Badge>
                <span className="text-xs text-gray-600">{results.length} records</span>
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => downloadResults(results, `query_results_${Date.now()}`, 'csv')}
                  className="text-xs flex-shrink-0"
                >
                  <FileText className="h-3 w-3 mr-1" />
                  CSV
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => downloadResults(results, `query_results_${Date.now()}`, 'excel')}
                  className="text-xs flex-shrink-0"
                >
                  <FileSpreadsheet className="h-3 w-3 mr-1" />
                  Excel
                </Button>
              </div>
            </div>

            {message.sqlQuery && (
              <div className="mt-3 bg-gray-100 p-3 rounded text-xs font-mono overflow-hidden">
                <span className="font-medium text-gray-700">SQL Query:</span>
                <div className="mt-1 text-gray-600 break-all overflow-wrap-anywhere">{message.sqlQuery}</div>
              </div>
            )}
          </div>

          <div className="overflow-x-auto max-h-96">
            <table className="w-full text-xs min-w-full">
              <thead className="bg-gray-50 sticky top-0">
                <tr>
                  {headers.map((header) => (
                    <th key={header} className="px-3 py-2 text-left font-medium text-gray-700 border-b border-gray-200 whitespace-nowrap">
                      {header}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {results.map((row, index) => (
                  <tr key={index} className={index % 2 === 0 ? "bg-white" : "bg-gray-50"}>
                    {headers.map((header) => (
                      <td key={header} className="px-3 py-2 border-b border-gray-100 text-gray-800 whitespace-nowrap">
                        {typeof row[header] === 'number' && (header.toLowerCase().includes('value') || header.toLowerCase().includes('rent') || header.toLowerCase().includes('income'))
                          ? `$${row[header]?.toLocaleString() || 0}`
                          : row[header]?.toString() || ""}
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          
          {results.length > 50 && (
            <div className="text-center py-3 text-xs text-gray-500 bg-gray-50 border-t">
              Showing all {results.length} results. Use CSV or Excel export for external analysis.
            </div>
          )}
        </div>
        
        {/* Only show external search summary if there's actual summary content different from main content */}
        {message.summary && message.summary !== message.content && (
          <div className="bg-blue-50 border border-blue-200 rounded-lg p-4 overflow-hidden">
            <div className="flex items-center space-x-2 mb-3">
              <FileText className="h-4 w-4 text-blue-600 flex-shrink-0" />
              <Badge variant="default">Data Analysis Summary</Badge>
            </div>
            <div 
              className="text-sm text-gray-800 leading-relaxed break-words overflow-wrap-anywhere"
              dangerouslySetInnerHTML={{ __html: renderMarkdown(message.summary) }}
            />
          </div>
        )}
      </div>
    )
  }

  const renderGroundedSearchResult = (message: Message) => {
    if (!message.isGrounded && message.queryType !== "grounded" && message.queryType !== "grounded_fallback") return null

    return (
      <div className="mt-4 space-y-4 w-full overflow-hidden">
        {/* Search Result Content */}
        <div className="bg-green-50 border border-green-200 rounded-lg p-4 overflow-hidden">
          <div className="flex items-center space-x-2 mb-3 flex-wrap">
            <Search className="h-4 w-4 text-green-600 flex-shrink-0" />
            <Badge variant="secondary">Web Search Results</Badge>
            {message.queryType === "grounded_fallback" && (
              <Badge variant="outline" className="text-xs">Fallback Search</Badge>
            )}
          </div>
          <div 
            className="text-sm text-gray-800 leading-relaxed prose prose-sm max-w-none break-words overflow-wrap-anywhere"
            dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }}
          />
        </div>

        {/* Sources Section */}
        {message.sources && message.sources.length > 0 && (
          <div className="bg-white border border-gray-200 rounded-lg p-4 overflow-hidden">
            <div className="flex items-center space-x-2 mb-3 flex-wrap">
              <ExternalLink className="h-4 w-4 text-blue-600 flex-shrink-0" />
              <Badge variant="outline">Sources</Badge>
              <span className="text-xs text-gray-600">{message.sources.length} references</span>
            </div>
            <div className="space-y-2">
              {message.sources.map((source, index) => (
                <a
                  key={index}
                  href={source.uri}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="flex items-start space-x-2 p-2 rounded bg-gray-50 hover:bg-gray-100 transition-colors overflow-hidden"
                >
                  <ExternalLink className="h-3 w-3 mt-1 text-blue-600 flex-shrink-0" />
                  <div className="flex-1 min-w-0 overflow-hidden">
                    <div className="text-sm font-medium text-blue-600 hover:text-blue-800 truncate">
                      {source.title}
                    </div>
                    <div className="text-xs text-gray-500 break-all overflow-wrap-anywhere">
                      {source.uri}
                    </div>
                  </div>
                </a>
              ))}
            </div>
          </div>
        )}
      </div>
    )
  }

  return (
    <DashboardLayout>
      <div className="h-[calc(100vh-8rem)] flex flex-col overflow-hidden">
        <div className="mb-6 flex-shrink-0">
          <h1 className="text-3xl font-bold">AI Assistant</h1>
          <p className="text-gray-600">
            Chat with AI for general questions or ask about real estate data - I'll automatically route your query!
          </p>
        </div>

        {error && (
          <Alert variant="destructive" className="mb-4 flex-shrink-0">
            <AlertDescription>{error}</AlertDescription>
          </Alert>
        )}

        <div className="flex gap-6 flex-1 min-h-0 overflow-hidden">
          {/* Main Chat Area */}
          <Card className="flex-1 flex flex-col min-w-0 overflow-hidden">
            <CardHeader className="flex-shrink-0">
              <CardTitle className="flex items-center space-x-2 flex-wrap">
                <Bot className="h-5 w-5 flex-shrink-0" />
                <span className="truncate">Partners8 AI Assistant</span>
                {databaseInfo?.available && (
                  <Badge variant="secondary" className="ml-2 flex-shrink-0">
                    <BarChart3 className="h-3 w-3 mr-1" />
                    {databaseInfo.total_rows?.toLocaleString()} records
                  </Badge>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col p-0 min-h-0 overflow-hidden">
              {/* Chat Messages */}
              <ScrollArea className="flex-1 p-4 overflow-hidden">
                <div className="space-y-6">
                  {messages.map((message) => (
                    <div key={message.id} className="w-full overflow-hidden">
                      {/* Regular Chat Message */}
                      <div className={`flex ${message.sender === "user" ? "justify-end" : "justify-start"} w-full`}>
                        <div
                          className={`max-w-[80%] min-w-0 rounded-lg p-3 overflow-hidden ${
                            message.sender === "user" ? "bg-blue-600 text-white" : "bg-gray-100 text-gray-900"
                          }`}
                        >
                          <div className="flex items-start space-x-2 min-w-0">
                            {message.sender === "bot" && <Bot className="h-4 w-4 mt-0.5 flex-shrink-0" />}
                            {message.sender === "user" && (
                              <User className="h-4 w-4 mt-0.5 flex-shrink-0 text-blue-100" />
                            )}
                            <div className="flex-1 min-w-0 overflow-hidden">
                              {message.queryType !== "data_query" && !message.isGrounded && message.queryType !== "grounded" && message.queryType !== "grounded_fallback" && (
                                <p className="text-sm whitespace-pre-wrap break-words overflow-wrap-anywhere">
                                  {message.content}
                                </p>
                              )}

                              {/* Query Type Badges */}
                              <div className="mt-2 flex flex-wrap gap-1">
                                {message.queryType === "data_query" && (
                                  <Badge variant="default" className="text-xs">
                                    <Database className="h-3 w-3 mr-1" />
                                    Data Query
                                  </Badge>
                                )}
                                {message.isGrounded && (
                                  <Badge variant="secondary" className="text-xs">
                                    <Search className="h-3 w-3 mr-1" />
                                    Grounded
                                  </Badge>
                                )}
                                {message.queryType === "grounded_fallback" && (
                                  <Badge variant="outline" className="text-xs">
                                    Search Fallback
                                  </Badge>
                                )}
                              </div>

                              <p
                                className={`text-xs mt-2 ${
                                  message.sender === "user" ? "text-blue-100" : "text-gray-500"
                                }`}
                              >
                                {formatTime(message.timestamp)}
                              </p>
                            </div>
                          </div>
                        </div>
                      </div>

                      {/* Render appropriate result type based on query type */}
                      {message.queryType === "data_query" ? 
                        renderDataQueryResult(message) : 
                        renderGroundedSearchResult(message)
                      }
                    </div>
                  ))}
                  
                  {isLoading && (
                    <div className="flex justify-start w-full">
                      <div className="bg-gray-100 rounded-lg p-3 max-w-[80%] overflow-hidden">
                        <div className="flex items-center space-x-2">
                          <Bot className="h-4 w-4 flex-shrink-0" />
                          <div className="flex space-x-1">
                            <div className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"></div>
                            <div
                              className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"
                              style={{ animationDelay: "0.1s" }}
                            ></div>
                            <div
                              className="w-2 h-2 bg-gray-400 rounded-full animate-bounce"
                              style={{ animationDelay: "0.2s" }}
                            ></div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                  <div ref={messagesEndRef} />
                </div>
              </ScrollArea>

              {/* Chat Input */}
              <div className="border-t p-4 flex-shrink-0">
                <div className="flex space-x-2 w-full min-w-0">
                  <Input
                    value={inputMessage}
                    onChange={(e) => setInputMessage(e.target.value)}
                    placeholder="Ask me anything about real estate data or general questions..."
                    disabled={isLoading}
                    className="flex-1 min-w-0"
                    onKeyDown={(e) => {
                      if (e.key === 'Enter' && !e.shiftKey) {
                        e.preventDefault()
                        sendChatMessage(e)
                      }
                    }}
                  />
                  <Button 
                    onClick={sendChatMessage} 
                    disabled={isLoading || !inputMessage.trim()}
                    className="flex-shrink-0"
                  >
                    <Send className="h-4 w-4" />
                  </Button>
                </div>
              </div>
            </CardContent>
          </Card>

          {/* Sidebar */}
          <div className="w-80 flex-shrink-0 space-y-4 overflow-y-auto max-h-full">
            {/* Database Status */}
            {databaseInfo && (
              <Card className="overflow-hidden">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center">
                    <Database className="h-4 w-4 mr-2 flex-shrink-0" />
                    <span className="truncate">Database Status</span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {databaseInfo.available ? (
                    <>
                      <div className="flex items-center justify-between text-sm">
                        <span>Status:</span>
                        <Badge variant="default">Available</Badge>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span>Records:</span>
                        <span className="font-medium">{databaseInfo.total_rows?.toLocaleString()}</span>
                      </div>
                      <div className="text-xs text-gray-600 break-words">
                        Real estate data from Zillow, HUD, and Census sources
                      </div>
                    </>
                  ) : (
                    <div className="text-sm text-gray-600">
                      <Badge variant="secondary">Unavailable</Badge>
                      <p className="mt-2 break-words">{databaseInfo.message}</p>
                    </div>
                  )}
                </CardContent>
              </Card>
            )}

            {/* Sample Queries */}
            {databaseInfo?.available && databaseInfo.sample_queries && (
              <Card className="overflow-hidden">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base truncate">Sample Queries</CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  {databaseInfo.sample_queries.map((query, index) => (
                    <Button
                      key={index}
                      variant="outline"
                      size="sm"
                      className="w-full text-left justify-start h-auto p-2 text-xs break-words whitespace-normal"
                      onClick={() => useSampleQuery(query)}
                    >
                      {query}
                    </Button>
                  ))}
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}