import client from "./client"

export interface LoginResponse {
  access_token: string
  token_type: string
}

export interface RegisterResponse {
  id: number
  username: string
  email: string
}

export async function login(username: string, password: string): Promise<LoginResponse> {
  const params = new URLSearchParams()
  params.append("username", username)
  params.append("password", password)
  const res = await client.post<LoginResponse>("/auth/login", params, {
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
  })
  return res.data
}

export async function register(username: string, email: string, password: string): Promise<RegisterResponse> {
  const res = await client.post<RegisterResponse>("/auth/register", { username, email, password })
  return res.data
}
