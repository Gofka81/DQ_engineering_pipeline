import { createContext, useContext, useState, useCallback } from "react"
import { login as apiLogin, register as apiRegister } from "../api/auth"

interface AuthContextValue {
  token: string | null
  username: string | null
  login: (username: string, password: string) => Promise<void>
  register: (username: string, email: string, password: string) => Promise<void>
  logout: () => void
}

export const AuthContext = createContext<AuthContextValue | null>(null)

export function useAuthState(): AuthContextValue {
  const [token, setToken] = useState<string | null>(() => localStorage.getItem("token"))
  const [username, setUsername] = useState<string | null>(() => localStorage.getItem("username"))

  const login = useCallback(async (u: string, p: string) => {
    const res = await apiLogin(u, p)
    sessionStorage.removeItem("selectedRunId")
    sessionStorage.removeItem("selectedFileId")
    sessionStorage.removeItem("liveRun")
    localStorage.setItem("token", res.access_token)
    localStorage.setItem("username", u)
    setToken(res.access_token)
    setUsername(u)
  }, [])

  const register = useCallback(async (u: string, email: string, p: string) => {
    await apiRegister(u, email, p)
  }, [])

  const logout = useCallback(() => {
    localStorage.removeItem("token")
    localStorage.removeItem("username")
    sessionStorage.removeItem("selectedRunId")
    sessionStorage.removeItem("selectedFileId")
    sessionStorage.removeItem("liveRun")
    setToken(null)
    setUsername(null)
  }, [])

  return { token, username, login, register, logout }
}

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error("useAuth must be used inside AuthContext.Provider")
  return ctx
}
