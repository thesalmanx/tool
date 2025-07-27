"use client"

import type React from "react"
import { useEffect, useState } from "react"
import { useRouter } from "next/navigation"
import DashboardLayout from "@/components/dashboard-layout"
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card"
import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import { Label } from "@/components/ui/label"
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select"
import { Badge } from "@/components/ui/badge"
import { Alert, AlertDescription } from "@/components/ui/alert"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogHeader,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table"
import { CheckCircle, Plus, UserPlus, Crown, Edit } from "lucide-react"

interface User {
  id: number
  username: string
  email: string
  role: string
  is_approved: boolean
  created_at: string
  created_by: number | null
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

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([])
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [newUser, setNewUser] = useState({
    username: "",
    email: "",
    password: "",
    role: "user",
    is_approved: true,
  })
  const router = useRouter()

  useEffect(() => {
    const initializeUsersPage = async () => {
      const token = getCookie("access_token")
      const role = getCookie("user_role")

      if (!token) {
        router.push("/")
        return
      }

      if (role !== "admin") {
        router.push("/dashboard")
        return
      }

      await fetchUsers()
    }

    initializeUsersPage()
  }, [router])

  const clearAllCookies = () => {
    deleteCookie("access_token")
    deleteCookie("username")
    deleteCookie("user_role")
    deleteCookie("user_id")
    deleteCookie("user_email")
  }

  const fetchUsers = async () => {
    try {
      const token = getCookie("access_token")
      const response = await fetch("http://localhost:8000/users", {
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        const data = await response.json()
        setUsers(data)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        setError("Failed to fetch users")
      }
    } catch (err) {
      setError("Network error")
    } finally {
      setIsLoading(false)
    }
  }

  const approveUser = async (userId: number) => {
    try {
      const token = getCookie("access_token")
      const response = await fetch(`http://localhost:8000/approve_user/${userId}`, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        setSuccess("User approved successfully")
        fetchUsers()
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        setError("Failed to approve user")
      }
    } catch (err) {
      setError("Network error")
    }
  }

  const promoteToAdmin = async (userId: number) => {
    try {
      const token = getCookie("access_token")
      const response = await fetch(`http://localhost:8000/promote_to_admin/${userId}`, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${token}`,
        },
      })

      if (response.ok) {
        setSuccess("User promoted to admin successfully")
        fetchUsers()
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        setError("Failed to promote user")
      }
    } catch (err) {
      setError("Network error")
    }
  }

  const updateUser = async (userId: number, updates: { role?: string; is_approved?: boolean }) => {
    try {
      const token = getCookie("access_token")
      const response = await fetch(`http://localhost:8000/users/${userId}`, {
        method: "PUT",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(updates),
      })

      if (response.ok) {
        setSuccess("User updated successfully")
        fetchUsers()
        setEditingUser(null)
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        setError("Failed to update user")
      }
    } catch (err) {
      setError("Network error")
    }
  }

  const createUser = async (e: React.FormEvent) => {
    e.preventDefault()
    try {
      const token = getCookie("access_token")
      const response = await fetch("http://localhost:8000/users", {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify(newUser),
      })

      if (response.ok) {
        setSuccess("User created successfully")
        setIsCreateDialogOpen(false)
        setNewUser({
          username: "",
          email: "",
          password: "",
          role: "user",
          is_approved: true,
        })
        fetchUsers()
        setTimeout(() => setSuccess(""), 3000)
      } else if (response.status === 401) {
        clearAllCookies()
        router.push("/")
      } else {
        const errorData = await response.json()
        setError(errorData.detail || "Failed to create user")
      }
    } catch (err) {
      setError("Network error")
    }
  }

  if (isLoading) {
    return (
      <DashboardLayout>
        <div className="flex items-center justify-center h-64">
          <div className="text-center">
            <div className="animate-spin rounded-full h-12 w-12 border-b-2 border-blue-600 mx-auto"></div>
            <p className="mt-4 text-gray-600">Loading users...</p>
          </div>
        </div>
      </DashboardLayout>
    )
  }

  return (
    <DashboardLayout>
      <div className="space-y-6">
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-3xl font-bold">User Management</h1>
            <p className="text-gray-600">Manage users, roles, and permissions</p>
          </div>
          <Dialog open={isCreateDialogOpen} onOpenChange={setIsCreateDialogOpen}>
            <DialogTrigger asChild>
              <Button>
                <Plus className="h-4 w-4 mr-2" />
                Create User
              </Button>
            </DialogTrigger>
            <DialogContent>
              <DialogHeader>
                <DialogTitle>Create New User</DialogTitle>
                <DialogDescription>Add a new user to the system with specified role and permissions.</DialogDescription>
              </DialogHeader>
              <form onSubmit={createUser} className="space-y-4">
                <div className="space-y-2">
                  <Label htmlFor="username">Username</Label>
                  <Input
                    id="username"
                    value={newUser.username}
                    onChange={(e) => setNewUser({ ...newUser, username: e.target.value })}
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="email">Email</Label>
                  <Input
                    id="email"
                    type="email"
                    value={newUser.email}
                    onChange={(e) => setNewUser({ ...newUser, email: e.target.value })}
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="password">Password</Label>
                  <Input
                    id="password"
                    type="password"
                    value={newUser.password}
                    onChange={(e) => setNewUser({ ...newUser, password: e.target.value })}
                    required
                  />
                </div>
                <div className="space-y-2">
                  <Label htmlFor="role">Role</Label>
                  <Select value={newUser.role} onValueChange={(value) => setNewUser({ ...newUser, role: value })}>
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="user">User</SelectItem>
                      <SelectItem value="admin">Admin</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <Button type="submit" className="w-full">
                  <UserPlus className="h-4 w-4 mr-2" />
                  Create User
                </Button>
              </form>
            </DialogContent>
          </Dialog>
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

        <Card>
          <CardHeader>
            <CardTitle>All Users</CardTitle>
            <CardDescription>Manage user accounts, roles, and approval status</CardDescription>
          </CardHeader>
          <CardContent>
            <Table>
              <TableHeader>
                <TableRow>
                  <TableHead>Username</TableHead>
                  <TableHead>Email</TableHead>
                  <TableHead>Role</TableHead>
                  <TableHead>Status</TableHead>
                  <TableHead>Created</TableHead>
                  <TableHead>Actions</TableHead>
                </TableRow>
              </TableHeader>
              <TableBody>
                {users.map((user) => (
                  <TableRow key={user.id}>
                    <TableCell className="font-medium">{user.username}</TableCell>
                    <TableCell>{user.email}</TableCell>
                    <TableCell>
                      <Badge variant={user.role === "admin" ? "default" : "secondary"}>
                        {user.role === "admin" && <Crown className="h-3 w-3 mr-1" />}
                        {user.role}
                      </Badge>
                    </TableCell>
                    <TableCell>
                      <Badge variant={user.is_approved ? "default" : "destructive"}>
                        {user.is_approved ? "Approved" : "Pending"}
                      </Badge>
                    </TableCell>
                    <TableCell>{new Date(user.created_at).toLocaleDateString()}</TableCell>
                    <TableCell>
                      <div className="flex space-x-2">
                        {!user.is_approved && (
                          <Button size="sm" onClick={() => approveUser(user.id)}>
                            <CheckCircle className="h-4 w-4 mr-1" />
                            Approve
                          </Button>
                        )}
                        {user.role !== "admin" && user.is_approved && (
                          <Button size="sm" variant="outline" onClick={() => promoteToAdmin(user.id)}>
                            <Crown className="h-4 w-4 mr-1" />
                            Promote
                          </Button>
                        )}
                        <Button size="sm" variant="outline" onClick={() => setEditingUser(user)}>
                          <Edit className="h-4 w-4 mr-1" />
                          Edit
                        </Button>
                      </div>
                    </TableCell>
                  </TableRow>
                ))}
              </TableBody>
            </Table>
          </CardContent>
        </Card>

        {/* Edit User Dialog */}
        <Dialog open={!!editingUser} onOpenChange={() => setEditingUser(null)}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle>Edit User</DialogTitle>
              <DialogDescription>Update user role and approval status.</DialogDescription>
            </DialogHeader>
            {editingUser && (
              <div className="space-y-4">
                <div className="space-y-2">
                  <Label>Username</Label>
                  <Input value={editingUser.username} disabled />
                </div>
                <div className="space-y-2">
                  <Label>Email</Label>
                  <Input value={editingUser.email} disabled />
                </div>
                <div className="space-y-2">
                  <Label>Role</Label>
                  <Select
                    value={editingUser.role}
                    onValueChange={(value) => setEditingUser({ ...editingUser, role: value })}
                  >
                    <SelectTrigger>
                      <SelectValue />
                    </SelectTrigger>
                    <SelectContent>
                      <SelectItem value="user">User</SelectItem>
                      <SelectItem value="admin">Admin</SelectItem>
                    </SelectContent>
                  </Select>
                </div>
                <div className="flex items-center space-x-2">
                  <input
                    type="checkbox"
                    id="approved"
                    checked={editingUser.is_approved}
                    onChange={(e) => setEditingUser({ ...editingUser, is_approved: e.target.checked })}
                  />
                  <Label htmlFor="approved">Approved</Label>
                </div>
                <Button
                  onClick={() =>
                    updateUser(editingUser.id, {
                      role: editingUser.role,
                      is_approved: editingUser.is_approved,
                    })
                  }
                  className="w-full"
                >
                  Update User
                </Button>
              </div>
            )}
          </DialogContent>
        </Dialog>
      </div>
    </DashboardLayout>
  )
}