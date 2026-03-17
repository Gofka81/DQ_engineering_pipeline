import { useState } from "react"
import { useAuth } from "../../hooks/useAuth"
import { useNavigate } from "react-router-dom"

export function AuthForm() {
  const [tab, setTab] = useState<"login" | "register">("login")
  const [username, setUsername] = useState("")
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState<string | null>(null)
  const [success, setSuccess] = useState<string | null>(null)
  const [loading, setLoading] = useState(false)
  const { login, register } = useAuth()
  const navigate = useNavigate()

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError(null)
    setSuccess(null)
    setLoading(true)
    try {
      if (tab === "login") {
        await login(username, password)
        navigate("/")
      } else {
        await register(username, email, password)
        setTab("login")
        setSuccess("Account created — please sign in.")
      }
    } catch (err: unknown) {
      const detail = (err as { response?: { data?: { detail?: unknown } } })?.response?.data?.detail
      const msg = typeof detail === "string"
        ? detail
        : Array.isArray(detail)
          ? (detail[0] as { msg?: string })?.msg ?? "Validation error"
          : "Something went wrong"
      setError(msg)
    } finally {
      setLoading(false)
    }
  }

  const inputCls = "w-full border border-gray-300 dark:border-zinc-600 rounded-lg px-3 py-2 text-sm bg-white dark:bg-zinc-800 dark:text-zinc-100 focus:outline-none focus:ring-2 focus:ring-blue-500"

  return (
    <div className="min-h-screen bg-gray-100 dark:bg-zinc-950 flex items-center justify-center">
      <div className="bg-white dark:bg-zinc-900 rounded-xl shadow-sm border border-gray-200 dark:border-zinc-700 w-full max-w-sm p-8">
        <h1 className="text-xl font-bold text-center text-gray-800 dark:text-zinc-100 mb-6">DQ Pipeline</h1>

        <div className="flex gap-1 mb-6 bg-gray-100 dark:bg-zinc-800 rounded-lg p-1">
          {(["login", "register"] as const).map((t) => (
            <button
              key={t}
              onClick={() => { setTab(t); setError(null); setSuccess(null) }}
              className={`flex-1 py-1.5 text-sm font-medium rounded-md transition-colors ${
                tab === t
                  ? "bg-white dark:bg-zinc-700 shadow-sm text-gray-800 dark:text-zinc-100"
                  : "text-gray-500 dark:text-zinc-400"
              }`}
            >
              {t === "login" ? "Login" : "Register"}
            </button>
          ))}
        </div>

        <form onSubmit={handleSubmit} className="flex flex-col gap-4">
          <div>
            <label className="block text-sm text-gray-600 dark:text-zinc-300 mb-1">Username</label>
            <input
              type="text"
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className={inputCls}
              required
            />
          </div>

          {tab === "register" && (
            <div>
              <label className="block text-sm text-gray-600 dark:text-zinc-300 mb-1">Email</label>
              <input
                type="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                className={inputCls}
                required
              />
            </div>
          )}

          <div>
            <label className="block text-sm text-gray-600 dark:text-zinc-300 mb-1">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className={inputCls}
              required
            />
          </div>

          {success && <p className="text-green-500 text-sm">{success}</p>}
          {error && <p className="text-red-500 text-sm">{error}</p>}

          <button
            type="submit"
            disabled={loading}
            className="bg-blue-600 text-white rounded-lg py-2 text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? "..." : tab === "login" ? "Sign In" : "Create Account"}
          </button>
        </form>
      </div>
    </div>
  )
}
