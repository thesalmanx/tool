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
import { 
  Send, Bot, User, Database, Search, ExternalLink, BarChart3, 
  FileText, Table, FileSpreadsheet, MapPin, TrendingUp, Home,
  DollarSign, Users, Building
} from "lucide-react"
import { apiClient } from "../../../utils/api"

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

interface EnhancedDatabaseInfo {
  available: boolean
  total_rows?: number
  unique_states?: number
  unique_zipcodes?: number
  landlord_friendly_states?: Record<string, string>
  columns?: Array<{ name: string; type: string }>
  sample_queries?: string[]
  key_features?: string[]
  message?: string
}

interface PredefinedQueryResult {
  success: boolean
  description: string
  results: any[]
  count: number
}

const LANDLORD_FRIENDLY_QUERIES = [
  {
    key: "landlord_friendly_highest_zh",
    title: "🏆 Highest ZH Ratio in Landlord-Friendly States",
    description: "Cities with the best cash flow opportunities",
    endpoint: "/queries/landlord-friendly-highest-zh"
  },
  {
    key: "landlord_friendly_population_100k",
    title: "🏙️ Major Cities with High ZH Ratio",
    description: "Large population centers with great investment potential",
    endpoint: "/queries/landlord-friendly-population-100k"
  },
  {
    key: "landlord_friendly_zipcodes",
    title: "📍 Top Investment Zipcodes",
    description: "Specific zipcodes with highest ZH ratios",
    endpoint: "/queries/landlord-friendly-zipcodes"
  }
]

export default function EnhancedChatPage() {
  const [messages, setMessages] = useState<Message[]>([])
  const [inputMessage, setInputMessage] = useState("")
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState("")
  const [currentSessionId, setCurrentSessionId] = useState<string | null>(null)
  const [databaseInfo, setDatabaseInfo] = useState<EnhancedDatabaseInfo | null>(null)
  const [isLoadingPredefined, setIsLoadingPredefined] = useState<string | null>(null)
  const messagesEndRef = useRef<HTMLDivElement>(null)
  const router = useRouter()

  useEffect(() => {
    loadDatabaseInfo()
    setMessages([
      {
        id: "welcome",
        content: "Welcome to the Enhanced Partners8 AI Assistant! 🏠\n\nI'm now equipped with comprehensive landlord-friendly state analysis. I can help you find the best real estate investment opportunities in the 18 most investor-friendly states.\n\n✨ **New Features:**\n• ZH Ratio analysis for cash flow optimization\n• Landlord-friendly state filtering\n• Population-based queries\n• Enhanced data visualizations\n\nTry asking about landlord-friendly states or use the quick query buttons below!",
        sender: "bot",
        timestamp: new Date(),
      },
    ])
  }, [])

  useEffect(() => {
    scrollToBottom()
  }, [messages])

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" })
  }

  const loadDatabaseInfo = async () => {
    try {
      const data = await apiClient.get("/database/info")
      setDatabaseInfo(data)
    } catch (err: any) {
      console.error("Failed to load database info:", err)
      if (err.message && err.message.includes("401")) {
        router.push("/")
      }
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
      const data: ChatResponse = await apiClient.post("/chat", {
        message: messageToSend,
        session_id: currentSessionId,
      })

      if (!currentSessionId) {
        setCurrentSessionId(data.session_id)
      }

      let summary = ""
      let cleanResponse = data.response
      
      if (data.query_type === "enhanced_data_query" && data.response.includes("**Enhanced Data Analysis:**")) {
        const parts = data.response.split("**Enhanced Data Analysis:**")
        if (parts.length > 1) {
          const summaryPart = parts[1].split("**Found")[0].trim()
          summary = summaryPart.replace(/\n\n/g, " ").trim()
          cleanResponse = summary
        }
      } else if (data.query_type === "landlord_friendly_query" && data.response.includes("**Landlord-Friendly States Analysis:**")) {
        const parts = data.response.split("**Landlord-Friendly States Analysis:**")
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
    } catch (err: any) {
      console.error("Failed to send message:", err)
      if (err.message && err.message.includes("401")) {
        router.push("/")
      } else {
        setError(err.message || "Failed to send message")
      }
    } finally {
      setIsLoading(false)
    }
  }

  const executePredefinedQuery = async (queryConfig: typeof LANDLORD_FRIENDLY_QUERIES[0]) => {
    setIsLoadingPredefined(queryConfig.key)
    setError("")

    try {
      const data: PredefinedQueryResult = await apiClient.get(queryConfig.endpoint)
      
      if (data.success) {
        // Create user message
        const userMessage: Message = {
          id: Date.now().toString(),
          content: queryConfig.title,
          sender: "user",
          timestamp: new Date(),
        }

        // Create bot response
        const botMessage: Message = {
          id: (Date.now() + 1).toString(),
          content: `**${queryConfig.title}**\n\n${data.description}\n\nFound ${data.count} results with the highest investment potential in landlord-friendly states.`,
          sender: "bot",
          timestamp: new Date(),
          queryType: "landlord_friendly_query",
          queryResults: data.results,
          summary: `Analysis of ${data.count} locations showing the best investment opportunities based on ZH Ratio analysis.`,
        }

        setMessages((prev) => [...prev, userMessage, botMessage])
        
        if (!currentSessionId) {
          // Generate a session ID for predefined queries
          setCurrentSessionId(`predefined-${Date.now()}`)
        }
      }
    } catch (err: any) {
      console.error("Failed to execute predefined query:", err)
      if (err.message && err.message.includes("401")) {
        router.push("/")
      } else {
        setError(err.message || "Failed to execute query")
      }
    } finally {
      setIsLoadingPredefined(null)
    }
  }

  const useSampleQuery = (query: string) => {
    setInputMessage(query)
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
      const ws: XLSX.WorkSheet = XLSX.utils.json_to_sheet(data)
      const workbook = XLSX.utils.book_new()
      XLSX.utils.book_append_sheet(workbook, ws, 'Data')

      const colWidths = headers.map((header) => {
        const maxDataWidth = Math.max(...data.map(row => String(row[header] || '').length))
        return { wch: Math.max(header.length, maxDataWidth, 10) }
      })
      ws['!cols'] = colWidths

      const wbout = XLSX.write(workbook, { bookType: 'xlsx', type: 'binary' })
      
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
    return text
      .replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>')
      .replace(/\*(.*?)\*/g, '<em>$1</em>')
      .replace(/\n/g, '<br />')
  }

  const renderEnhancedDataResult = (message: Message) => {
    if (!["enhanced_data_query", "landlord_friendly_query"].includes(message.queryType || "") || !message.queryResults) return null

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
    const isLandlordQuery = message.queryType === "landlord_friendly_query"

    return (
      <div className="mt-4 space-y-4">
        {/* Enhanced Data Table Section */}
        <div className="w-full max-w-full bg-white border border-gray-200 rounded-lg overflow-hidden">
          <div className="border-b border-gray-200 p-4">
            <div className="flex items-center justify-between flex-wrap gap-2">
              <div className="flex items-center space-x-2 flex-wrap">
                {isLandlordQuery ? (
                  <Home className="h-4 w-4 text-green-600 flex-shrink-0" />
                ) : (
                  <Table className="h-4 w-4 text-green-600 flex-shrink-0" />
                )}
                <Badge variant={isLandlordQuery ? "default" : "secondary"}>
                  {isLandlordQuery ? "Landlord-Friendly Analysis" : "Enhanced Query Results"}
                </Badge>
                <span className="text-xs text-gray-600">{results.length} records</span>
                {isLandlordQuery && (
                  <Badge variant="outline" className="text-xs">
                    <MapPin className="h-3 w-3 mr-1" />
                    18 States
                  </Badge>
                )}
              </div>
              <div className="flex gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => downloadResults(results, `${isLandlordQuery ? 'landlord_friendly' : 'query'}_results_${Date.now()}`, 'csv')}
                  className="text-xs flex-shrink-0"
                >
                  <FileText className="h-3 w-3 mr-1" />
                  CSV
                </Button>
                <Button
                  size="sm"
                  variant="outline"
                  onClick={() => downloadResults(results, `${isLandlordQuery ? 'landlord_friendly' : 'query'}_results_${Date.now()}`, 'excel')}
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

            {isLandlordQuery && (
              <div className="mt-3 bg-green-50 p-3 rounded text-xs">
                <span className="font-medium text-green-800">Landlord-Friendly States:</span>
                <div className="mt-1 text-green-700">
                  AZ, AL, FL, GA, IN, CO, TX, NC, IL, KY, MI, NV, WV, TN, AK, LA, MN, WY
                </div>
              </div>
            )}
          </div>

          <div className="overflow-x-auto max-h-96">
            <table className="w-auto min-w-0 table-auto text-xs min-w-full">
              <thead className="bg-gray-50 sticky top-0">
                <tr>
                  {headers.map((header) => (
                    <th key={header} className="px-3 py-2 text-left font-medium text-gray-700 border-b border-gray-200 whitespace-nowrap">
                      {header === 'ZH Ratio' && <TrendingUp className="h-3 w-3 inline mr-1" />}
                      {header === 'RegionName' && <Building className="h-3 w-3 inline mr-1" />}
                      {header === 'State' && <MapPin className="h-3 w-3 inline mr-1" />}
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
                        {/* Special formatting for key columns */}
                        {header === 'ZH Ratio' && row[header] && (
                          <span className="font-medium text-green-600">
                            {typeof row[header] === 'string' ? row[header] : row[header]?.toFixed(4)}
                          </span>
                        )}
                        {header === 'State' && row[header] && (
                          <Badge variant="outline" className="text-xs">
                            {row[header]}
                          </Badge>
                        )}
                        {header === 'RegionName' && row[header] && (
                          <span className="font-medium text-blue-600">
                            {row[header]}
                          </span>
                        )}
                        {!['ZH Ratio', 'State', 'RegionName'].includes(header) && (
                          <>
                            {typeof row[header] === 'number' && (header.toLowerCase().includes('value') || header.toLowerCase().includes('rent') || header.toLowerCase().includes('income'))
                              ? `${row[header]?.toLocaleString() || 0}`
                              : row[header]?.toString() || "N/A"}
                          </>
                        )}
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
        
        {/* Enhanced Summary Section */}
        {message.summary && message.summary !== message.content && (
          <div className={`border rounded-lg p-4 overflow-hidden ${
            isLandlordQuery ? 'bg-green-50 border-green-200' : 'bg-blue-50 border-blue-200'
          }`}>
            <div className="flex items-center space-x-2 mb-3">
              {isLandlordQuery ? (
                <Home className="h-4 w-4 text-green-600 flex-shrink-0" />
              ) : (
                <FileText className="h-4 w-4 text-blue-600 flex-shrink-0" />
              )}
              <Badge variant="default">
                {isLandlordQuery ? "Investment Analysis Summary" : "Data Analysis Summary"}
              </Badge>
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
    if (!message.isGrounded && !["grounded", "grounded_fallback"].includes(message.queryType || "")) return null

    return (
      <div className="mt-4 space-y-4 w-full overflow-hidden">
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
          <h1 className="text-3xl font-bold">Enhanced AI Assistant</h1>
          <p className="text-gray-600">
            Advanced real estate analysis with landlord-friendly state insights and ZH Ratio optimization
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
                <span className="truncate">Enhanced Partners8 AI</span>
                {databaseInfo?.available && (
                  <>
                    <Badge variant="secondary" className="ml-2 flex-shrink-0">
                      <BarChart3 className="h-3 w-3 mr-1" />
                      {databaseInfo.total_rows?.toLocaleString()} records
                    </Badge>
                    <Badge variant="default" className="flex-shrink-0">
                      <Home className="h-3 w-3 mr-1" />
                      {Object.keys(databaseInfo.landlord_friendly_states || {}).length} Landlord-Friendly States
                    </Badge>
                  </>
                )}
              </CardTitle>
            </CardHeader>
            <CardContent className="flex-1 flex flex-col p-0 min-h-0 overflow-hidden">
              {/* Chat Messages */}
              <ScrollArea className="flex-1 p-4">
                <div className="space-y-6 w-full max-w-screen-lg mx-auto overflow-auto">
                  {messages.map((message) => (
                    <div key={message.id} className="w-full min-w-0">
                      {/* Regular Chat Message */}
                      <div className={`flex ${message.sender === "user" ? "justify-end" : "justify-start"} w-full min-w-0`}>
                        <div
                          className={`max-w-[80%] min-w-0 rounded-lg p-3 overflow-auto ${
                            message.sender === "user" ? "bg-blue-600 text-white" : "bg-gray-100 text-gray-900"
                          }`}
                        >
                          <div className="flex items-start space-x-2 min-w-0">
                            {message.sender === "bot" && <Bot className="h-4 w-4 mt-0.5 flex-shrink-0" />}
                            {message.sender === "user" && (
                              <User className="h-4 w-4 mt-0.5 flex-shrink-0 text-blue-100" />
                            )}
                            <div className="flex-1 min-w-0">
                              {!["enhanced_data_query", "landlord_friendly_query"].includes(message.queryType || "") && 
                               !message.isGrounded && !["grounded", "grounded_fallback"].includes(message.queryType || "") && (
                                <div className="overflow-auto">
                                  <div 
                                    className="text-sm whitespace-pre-wrap break-words overflow-wrap-anywhere"
                                    dangerouslySetInnerHTML={{ __html: renderMarkdown(message.content) }}
                                  />
                                </div>
                              )}

                              {/* Enhanced Query Type Badges */}
                              <div className="mt-2 flex flex-wrap gap-1 overflow-x-auto">
                                {message.queryType === "enhanced_data_query" && (
                                  <Badge variant="default" className="text-xs flex-shrink-0">
                                    <Database className="h-3 w-3 mr-1" />
                                    Enhanced Query
                                  </Badge>
                                )}
                                {message.queryType === "landlord_friendly_query" && (
                                  <Badge variant="default" className="text-xs flex-shrink-0">
                                    <Home className="h-3 w-3 mr-1" />
                                    Landlord-Friendly
                                  </Badge>
                                )}
                                {message.isGrounded && (
                                  <Badge variant="secondary" className="text-xs flex-shrink-0">
                                    <Search className="h-3 w-3 mr-1" />
                                    Grounded
                                  </Badge>
                                )}
                                {message.queryType === "grounded_fallback" && (
                                  <Badge variant="outline" className="text-xs flex-shrink-0">
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

                      {/* Render appropriate result type */}
                      <div className="w-full">
                        <div className="mt-2 w-full overflow-x-auto">
                          {["enhanced_data_query", "landlord_friendly_query"].includes(message.queryType || "") ? 
                            renderEnhancedDataResult(message) : 
                            renderGroundedSearchResult(message)
                          }
                        </div>
                      </div>
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
                    placeholder="Ask about landlord-friendly states, ZH ratios, or any real estate data..."
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

          {/* Enhanced Sidebar */}
          <div className="w-80 flex-shrink-0 space-y-4 overflow-y-auto max-h-full">
            {/* Quick Investment Queries */}
            <Card className="overflow-hidden">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center">
                  <TrendingUp className="h-4 w-4 mr-2 flex-shrink-0" />
                  <span className="truncate">Quick Investment Queries</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2">
                {LANDLORD_FRIENDLY_QUERIES.map((query) => (
                  <Button
                    key={query.key}
                    variant="outline"
                    size="sm"
                    className="w-full text-left justify-start h-auto p-3 text-xs break-words whitespace-normal"
                    onClick={() => executePredefinedQuery(query)}
                    disabled={isLoadingPredefined === query.key || isLoading}
                  >
                    <div className="flex flex-col items-start w-full">
                      <div className="font-medium">{query.title}</div>
                      <div className="text-gray-500 mt-1">{query.description}</div>
                    </div>
                    {isLoadingPredefined === query.key && (
                      <div className="ml-2 w-4 h-4 border-2 border-blue-500 border-t-transparent rounded-full animate-spin flex-shrink-0" />
                    )}
                  </Button>
                ))}
              </CardContent>
            </Card>

            {/* Enhanced Database Status */}
            {databaseInfo && (
              <Card className="overflow-hidden">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center">
                    <Database className="h-4 w-4 mr-2 flex-shrink-0" />
                    <span className="truncate">Enhanced Database</span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-3">
                  {databaseInfo.available ? (
                    <>
                      <div className="flex items-center justify-between text-sm">
                        <span>Status:</span>
                        <Badge variant="default">Enhanced</Badge>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span>Records:</span>
                        <span className="font-medium">{databaseInfo.total_rows?.toLocaleString()}</span>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span>States:</span>
                        <span className="font-medium">{databaseInfo.unique_states}</span>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span>Zipcodes:</span>
                        <span className="font-medium">{databaseInfo.unique_zipcodes?.toLocaleString()}</span>
                      </div>
                      <div className="flex items-center justify-between text-sm">
                        <span>Landlord-Friendly:</span>
                        <Badge variant="default" className="flex items-center">
                          <Home className="h-3 w-3 mr-1" />
                          {Object.keys(databaseInfo.landlord_friendly_states || {}).length}
                        </Badge>
                      </div>
                      <div className="text-xs text-gray-600 break-words">
                        Enhanced with ZH Ratio analysis, landlord-friendly state filtering, and comprehensive investment metrics
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

            {/* Landlord-Friendly States */}
            {databaseInfo?.landlord_friendly_states && (
              <Card className="overflow-hidden">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center">
                    <MapPin className="h-4 w-4 mr-2 flex-shrink-0" />
                    <span className="truncate">Landlord-Friendly States</span>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-2">
                  <div className="grid grid-cols-3 gap-1 text-xs">
                    {Object.entries(databaseInfo.landlord_friendly_states).map(([code, name]) => (
                      <Badge key={code} variant="outline" className="text-xs justify-center">
                        {code}
                      </Badge>
                    ))}
                  </div>
                  <div className="text-xs text-gray-600">
                    States identified as having favorable landlord and investment policies
                  </div>
                </CardContent>
              </Card>
            )}

            {/* Enhanced Ratio Definitions */}
            <Card className="overflow-hidden">
              <CardHeader className="pb-3">
                <CardTitle className="text-base flex items-center">
                  <BarChart3 className="h-4 w-4 mr-2 flex-shrink-0" />
                  <span className="truncate">Key Investment Metrics</span>
                </CardTitle>
              </CardHeader>
              <CardContent className="space-y-2 text-sm text-gray-600">
                <div className="bg-green-50 p-2 rounded text-xs">
                  <p><strong className="text-green-800">ZH Ratio:</strong> HUD 4-bedroom rent ÷ Zillow home value. Higher = better cash flow potential.</p>
                </div>
                <p><strong>Zillow Ratio:</strong> Monthly rent ÷ Zillow home value</p>
                <p><strong>NAR Ratio:</strong> Monthly rent ÷ NAR home value</p>
                <p><strong>NH Ratio:</strong> HUD 4-bedroom rent ÷ NAR home value</p>
                <div className="text-xs text-gray-500 mt-2">
                  💡 Focus on ZH Ratio for the best investment analysis
                </div>
              </CardContent>
            </Card>

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

            {/* Key Features */}
            {databaseInfo?.key_features && (
              <Card className="overflow-hidden">
                <CardHeader className="pb-3">
                  <CardTitle className="text-base flex items-center">
                    <Users className="h-4 w-4 mr-2 flex-shrink-0" />
                    <span className="truncate">Enhanced Features</span>
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-1 text-xs">
                    {databaseInfo.key_features.map((feature, index) => (
                      <div key={index} className="flex items-center space-x-2">
                        <div className="w-1.5 h-1.5 bg-green-500 rounded-full flex-shrink-0"></div>
                        <span className="text-gray-700">{feature}</span>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            )}
          </div>
        </div>
      </div>
    </DashboardLayout>
  )
}
