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
import { CheckCircle, Plus, UserPlus, Crown, Edit, Trash2, AlertTriangle, Users } from "lucide-react"
import { apiClient } from "../../../utils/api"

interface User {
  id: number
  username: string
  email: string
  role: string
  is_approved: boolean
  created_at: string
  created_by: number | null
}

import { getCookie, clearAuthCookies } from "../../../utils/cookies";

export default function UsersPage() {
  const [users, setUsers] = useState<User[]>([])
  const [currentPage, setCurrentPage] = useState(1)
  const [totalPages, setTotalPages] = useState(1)
  const itemsPerPage = 10
  const [isLoading, setIsLoading] = useState(true)
  const [error, setError] = useState("")
  const [success, setSuccess] = useState("")
  const [isCreateDialogOpen, setIsCreateDialogOpen] = useState(false)
  const [editingUser, setEditingUser] = useState<User | null>(null)
  const [deletingUser, setDeletingUser] = useState<User | null>(null)
  const [deleteConfirmText, setDeleteConfirmText] = useState("")
  const [newUser, setNewUser] = useState({
    username: "",
    email: "",
    password: "",
    role: "user",
    is_approved: true,
  })
  const [currentUserId, setCurrentUserId] = useState<string | null>(null)
  const [currentUserEmail, setCurrentUserEmail] = useState<string | null>(null)
  const [currentUserRole, setCurrentUserRole] = useState<string | null>(null)
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

      setCurrentUserId(getCookie("user_id"))
      setCurrentUserEmail(getCookie("user_email"))
      setCurrentUserRole(role)

      await fetchUsers()
    }

    initializeUsersPage()
  }, [router])

  

  const fetchUsers = async (page: number = currentPage) => {
    try {
      const data = await apiClient.get(`/users?page=${page}&limit=${itemsPerPage}`)
      setUsers(data.users)
      setTotalPages(Math.ceil(data.total / itemsPerPage))
      setCurrentPage(data.page)
    } catch (err: any) {
      console.error("Failed to fetch users:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      }
      setError(err.message || "Failed to fetch users")
    } finally {
      setIsLoading(false)
    }
  }

  const approveUser = async (userId: number) => {
    try {
      await apiClient.put(`/approve_user/${userId}`)
      setSuccess("User approved successfully")
      fetchUsers()
      setTimeout(() => setSuccess(""), 3000)
    } catch (err: any) {
      console.error("Failed to approve user:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to approve user")
      }
    }
  }

  const promoteToAdmin = async (userId: number) => {
    try {
      await apiClient.put(`/promote_to_admin/${userId}`)
      setSuccess("User promoted to admin successfully")
      fetchUsers()
      setTimeout(() => setSuccess(""), 3000)
    } catch (err: any) {
      console.error("Failed to promote user:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to promote user")
      }
    }
  }

  const updateUser = async (userId: number, updates: { role?: string; is_approved?: boolean }) => {
    try {
      await apiClient.put(`/users/${userId}`, updates)
      setSuccess("User updated successfully")
      fetchUsers()
      setEditingUser(null)
      setTimeout(() => setSuccess(""), 3000)
    } catch (err: any) {
      console.error("Failed to update user:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to update user")
      }
    }
  }

  const deleteUser = async (userId: number) => {
    try {
      const response = await apiClient.delete(`/users/${userId}`)
      setSuccess(`User deleted successfully: ${response.deleted_user?.username}`)
      fetchUsers()
      setDeletingUser(null)
      setDeleteConfirmText("")
      setTimeout(() => setSuccess(""), 5000)
    } catch (err: any) {
      console.error("Failed to delete user:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to delete user")
      }
    }
  }

 

  const createUser = async (e: React.FormEvent) => {
    e.preventDefault()
    try {
      await apiClient.post("/users", newUser)
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
    } catch (err: any) {
      console.error("Failed to create user:", err)
      if (err.message && err.message.includes("401")) {
        clearAuthCookies()
        router.push("/")
      } else {
        setError(err.message || "Failed to create user")
      }
    }
  }

  const canDeleteUser = (user: User): { canDelete: boolean; reason?: string } => {
    // Can't delete yourself
    if (user.id.toString() === currentUserId) {
      return { canDelete: false, reason: "Cannot delete your own account" }
    }

    // Only super admin can delete admins
    if (user.role === "admin" && currentUserRole !== "super_admin") {
      return { canDelete: false, reason: "Only super admins can delete admin accounts" }
    }

    return { canDelete: true }
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
          <div className="flex gap-2">
           
            
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

        {/* Current User Info */}
        {(currentUserId || currentUserEmail) && (
          <Card>
            <CardHeader>
              <CardTitle>Your Info</CardTitle>
              <CardDescription>Details of the currently logged-in user</CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-2 text-sm">
                {currentUserId && (
                  <p><strong>User ID:</strong> {currentUserId}</p>
                )}
                {currentUserEmail && (
                  <p><strong>Email:</strong> {currentUserEmail}</p>
                )}
                {currentUserRole && (
                  <p><strong>Role:</strong> {currentUserRole}</p>
                )}
              </div>
            </CardContent>
          </Card>
        )}

        <Card>
          <CardHeader>
            <CardTitle className="flex items-center justify-between">
              <span>All Users</span>
              {users.length > 0 && (
                <div className="flex items-center gap-2">
                  
                  <Badge variant="secondary">
                    <Users className="h-3 w-3 mr-1" />
                    {users.length} total
                  </Badge>
                </div>
              )}
            </CardTitle>
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
                {users.map((user) => {
                  const deletePermission = canDeleteUser(user)
                  return (
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
                          {deletePermission.canDelete ? (
                            <Button 
                              size="sm" 
                              variant="destructive" 
                              onClick={() => setDeletingUser(user)}
                            >
                              <Trash2 className="h-4 w-4 mr-1" />
                              Delete
                            </Button>
                          ) : (
                            <Button 
                              size="sm" 
                              variant="outline" 
                              disabled
                              title={deletePermission.reason}
                            >
                              <Trash2 className="h-4 w-4 mr-1" />
                              Delete
                            </Button>
                          )}
                        </div>
                      </TableCell>
                    </TableRow>
                  )
                })}
              </TableBody>
            </Table>
            <div className="flex justify-between items-center mt-4">
              <Button
                onClick={() => fetchUsers(currentPage - 1)}
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
                onClick={() => fetchUsers(currentPage + 1)}
                disabled={currentPage === totalPages || isLoading}
                variant="outline"
                size="sm"
              >
                Next
              </Button>
            </div>
          </CardContent>
        </Card>

        {/* Delete User Dialog */}
        <Dialog open={!!deletingUser} onOpenChange={() => {
          setDeletingUser(null)
          setDeleteConfirmText("")
        }}>
          <DialogContent>
            <DialogHeader>
              <DialogTitle className="flex items-center gap-2">
                <AlertTriangle className="h-5 w-5 text-red-500" />
                Delete User Account
              </DialogTitle>
              <DialogDescription>
                This action will permanently delete the user account and cannot be undone.
              </DialogDescription>
            </DialogHeader>
            {deletingUser && (
              <div className="space-y-4">
                <div className="bg-red-50 border border-red-200 rounded p-3">
                  <p className="text-sm text-red-800">
                    <strong>Warning:</strong> You are about to delete the user account for <strong>{deletingUser.username}</strong>.
                  </p>
                  <p className="text-sm text-red-600 mt-1">
                    This will also delete:
                  </p>
                  <ul className="text-sm text-red-600 mt-1 ml-4 list-disc">
                    <li>All chat sessions and messages</li>
                    <li>Scraping logs started by this user</li>
                    <li>All associated user data</li>
                  </ul>
                </div>
                
                <div className="space-y-2">
                  <Label htmlFor="confirmDelete">
                    Type <strong>{deletingUser.username}</strong> to confirm deletion:
                  </Label>
                  <Input
                    id="confirmDelete"
                    value={deleteConfirmText}
                    onChange={(e) => setDeleteConfirmText(e.target.value)}
                    placeholder={`Type "${deletingUser.username}" here`}
                    autoComplete="off"
                  />
                </div>
                
                <div className="flex gap-2">
                  <Button
                    variant="outline"
                    onClick={() => {
                      setDeletingUser(null)
                      setDeleteConfirmText("")
                    }}
                    className="flex-1"
                  >
                    Cancel
                  </Button>
                  <Button
                    variant="destructive"
                    onClick={() => deleteUser(deletingUser.id)}
                    disabled={deleteConfirmText !== deletingUser.username}
                    className="flex-1"
                  >
                    <Trash2 className="h-4 w-4 mr-2" />
                    Delete User
                  </Button>
                </div>
              </div>
            )}
          </DialogContent>
        </Dialog>

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
                    className="rounded border-gray-300"
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