import { useState } from "react"
import type { RunStatus } from "../../types"
import { StatusBadge } from "../status/StatusBadge"

interface Props {
  runId: string
  filename: string
  status: RunStatus
  createdAt: string
  storageExpired?: boolean
  onDownload?: () => void
}

export function RunHeader({ runId, filename, status, createdAt, storageExpired, onDownload }: Props) {
  const [copied, setCopied] = useState(false)

  function copyId() {
    navigator.clipboard.writeText(runId)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  const date = new Date(createdAt).toLocaleDateString("en-US", {
    month: "short", day: "numeric", year: "numeric",
    hour: "2-digit", minute: "2-digit",
  })

  return (
    <div className="bg-white dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-700 px-6 py-4">
      <div className="flex items-start justify-between gap-4">
        {/* Left: run ID + filename, then status + date */}
        <div className="flex items-start gap-6">
          <div className="min-w-0">
            <button
              onClick={copyId}
              className="flex items-center gap-2 group mb-1"
              title="Click to copy full run ID"
            >
              <span className="font-mono text-base font-semibold text-gray-800 dark:text-zinc-100">
                #{runId.slice(0, 8)}
              </span>
              <span className="font-mono text-xs text-gray-400 dark:text-zinc-500 truncate max-w-xs hidden sm:block">
                {runId.slice(8)}
              </span>
              <span className="text-xs text-gray-400 dark:text-zinc-500 opacity-0 group-hover:opacity-100 transition-opacity">
                {copied ? "copied!" : "copy"}
              </span>
            </button>
            <p className="text-sm text-gray-500 dark:text-zinc-400">{filename}</p>
          </div>
          <div className="flex flex-col gap-1 pt-0.5 flex-shrink-0">
            <StatusBadge status={status} />
            <span className="text-xs text-gray-400 dark:text-zinc-500">{date}</span>
          </div>
        </div>
        {/* Right: download button */}
        {status === "COMPLETED" && (
          storageExpired ? (
            <span
              title="File expired — curated files are kept for 14 days"
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-gray-200 dark:bg-zinc-700 text-gray-400 dark:text-zinc-500 rounded-lg font-medium cursor-not-allowed flex-shrink-0 self-center select-none"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              Expired
            </span>
          ) : onDownload ? (
            <button
              onClick={onDownload}
              className="flex items-center gap-1.5 px-3 py-1.5 text-sm bg-green-600 text-white rounded-lg hover:bg-green-700 font-medium transition-colors flex-shrink-0 self-center"
            >
              <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
              </svg>
              Download CSV
            </button>
          ) : null
        )}
      </div>
    </div>
  )
}
