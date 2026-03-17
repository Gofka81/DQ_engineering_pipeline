import axios from "axios"

const API_URL = import.meta.env.VITE_API_URL || "http://localhost:8000"

const client = axios.create({
  baseURL: `${API_URL}/api`,
})

client.interceptors.request.use((config) => {
  const token = localStorage.getItem("token")
  if (token) {
    config.headers.Authorization = `Bearer ${token}`
  }
  return config
})

client.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      localStorage.removeItem("token")
      localStorage.removeItem("username")
      sessionStorage.removeItem("selectedRunId")
      sessionStorage.removeItem("selectedFileId")
      sessionStorage.removeItem("liveRun")
      window.location.href = "/login"
    }
    return Promise.reject(err)
  }
)

export { API_URL }
export default client
