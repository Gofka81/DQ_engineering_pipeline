import { useAuth } from "../../hooks/useAuth"
import { useTheme } from "../../hooks/useTheme"
import { useNavigate } from "react-router-dom"

interface Props {
  sidebar: React.ReactNode
  children: React.ReactNode
}

function SunIcon() {
  return (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <circle cx="12" cy="12" r="4" strokeWidth="2" strokeLinecap="round" />
      <path strokeLinecap="round" strokeWidth="2" d="M12 2v2M12 20v2M4.22 4.22l1.42 1.42M18.36 18.36l1.42 1.42M2 12h2M20 12h2M4.22 19.78l1.42-1.42M18.36 5.64l1.42-1.42" />
    </svg>
  )
}

function MoonIcon() {
  return (
    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth="2" d="M21 12.79A9 9 0 1111.21 3 7 7 0 0021 12.79z" />
    </svg>
  )
}

export function AppShell({ sidebar, children }: Props) {
  const { username, logout } = useAuth()
  const { theme, toggle } = useTheme()
  const navigate = useNavigate()

  function handleLogout() {
    logout()
    navigate("/login")
  }

  return (
    <div className="flex h-screen overflow-hidden">
      {/* Sidebar */}
      <div className="w-64 flex-shrink-0 bg-zinc-900 flex flex-col">
        <div className="flex-1 overflow-y-auto min-h-0">
          {sidebar}
        </div>
        {/* Footer: username + controls */}
        <div className="flex-shrink-0 border-t border-zinc-700 px-4 py-4 flex items-center gap-3">
          <div className="w-8 h-8 rounded-full bg-zinc-700 flex items-center justify-center flex-shrink-0">
            <span className="text-xs font-semibold text-zinc-300 uppercase">
              {username?.slice(0, 1) ?? "?"}
            </span>
          </div>
          <span className="text-sm text-zinc-300 truncate flex-1">{username}</span>
          <button
            onClick={toggle}
            title={theme === "dark" ? "Switch to light mode" : "Switch to dark mode"}
            className="text-zinc-400 hover:text-zinc-200 transition-colors flex-shrink-0"
          >
            {theme === "dark" ? <SunIcon /> : <MoonIcon />}
          </button>
          <button
            onClick={handleLogout}
            className="text-sm text-zinc-500 hover:text-zinc-200 transition-colors flex-shrink-0"
          >
            Logout
          </button>
        </div>
      </div>
      {/* Main */}
      <div className="flex-1 flex flex-col overflow-hidden h-full bg-gray-50 dark:bg-zinc-950">
        {children}
      </div>
    </div>
  )
}
