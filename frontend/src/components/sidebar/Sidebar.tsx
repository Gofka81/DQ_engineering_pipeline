import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { listFilesWithLatestRun } from "../../api/files"
import type { RunStatus } from "../../types"

const PAGE_SIZE = 20

interface SidebarRun {
  runId: string
  fileId: string
  filename: string
  status: RunStatus
  createdAt: string
}

interface Props {
  selectedRunId: string | null
  activeRunId: string | null
  activeStatus: RunStatus | null
  onSelect: (runId: string, fileId: string) => void
  onNewAnalysis: () => void
}

const STATUS_DOT: Record<RunStatus, string> = {
  PENDING: "bg-zinc-500",
  ANALYZING: "bg-blue-400 animate-pulse",
  AWAITING_REVIEW: "bg-amber-400",
  TRANSFORMING: "bg-blue-400 animate-pulse",
  COMPLETED: "bg-green-400",
  FAILED: "bg-red-400",
}

function groupByDate(runs: SidebarRun[]): { label: string; runs: SidebarRun[] }[] {
  const now = new Date()
  const today = new Date(now.getFullYear(), now.getMonth(), now.getDate())
  const yesterday = new Date(today.getTime() - 86400000)
  const last7 = new Date(today.getTime() - 7 * 86400000)

  const groups: Record<string, SidebarRun[]> = {
    Today: [],
    Yesterday: [],
    "Last 7 days": [],
    Older: [],
  }

  for (const run of runs) {
    const d = new Date(run.createdAt)
    const day = new Date(d.getFullYear(), d.getMonth(), d.getDate())
    if (day >= today) groups["Today"].push(run)
    else if (day >= yesterday) groups["Yesterday"].push(run)
    else if (day >= last7) groups["Last 7 days"].push(run)
    else groups["Older"].push(run)
  }

  return Object.entries(groups)
    .filter(([, items]) => items.length > 0)
    .map(([label, items]) => ({ label, runs: items }))
}

export function Sidebar({ selectedRunId, activeRunId, activeStatus, onSelect, onNewAnalysis }: Props) {
  const [fileLimit, setFileLimit] = useState(PAGE_SIZE)

  const { data: files = [], isFetching } = useQuery({
    queryKey: ["files-with-latest-run", fileLimit],
    queryFn: () => listFilesWithLatestRun(0, fileLimit),
    staleTime: 30000,
  })

  const allRuns: SidebarRun[] = files
    .filter((f) => f.run_id !== null)
    .map((f) => ({
      runId: f.run_id!,
      fileId: f.id,
      filename: f.original_filename,
      status: f.run_id === activeRunId && activeStatus ? activeStatus : f.run_status!,
      createdAt: f.run_created_at!,
    }))
    .sort((a, b) => new Date(b.createdAt).getTime() - new Date(a.createdAt).getTime())

  const groups = groupByDate(allRuns)
  const runsLoading = isFetching && files.length === 0
  const hasMore = files.length === fileLimit

  return (
    <div className="flex flex-col h-full">
      {/* Header — py-7 + text-base line-height = 80px, matches RunHeader py-4 + two lines */}
      <div className="px-6 py-7 border-b border-zinc-700 flex-shrink-0">
        <span className="text-white font-semibold text-base">DQ Pipeline</span>
      </div>

      {/* Run list */}
      <div className="flex-1 overflow-y-auto px-2 pb-4">
        {/* New Analysis button */}
        <button
          onClick={onNewAnalysis}
          className="w-full flex items-center gap-2 px-3 py-2 mt-3 mb-3 rounded-lg bg-zinc-800 hover:bg-zinc-700 text-zinc-200 text-sm font-medium transition-colors border border-zinc-700"
        >
          <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 4v16m8-8H4" />
          </svg>
          New Analysis
        </button>

        {runsLoading && allRuns.length === 0 && (
          <p className="text-xs text-zinc-500 px-2 py-4">Loading runs…</p>
        )}
        {!runsLoading && allRuns.length === 0 && (
          <p className="text-xs text-zinc-500 px-2 py-4">No runs yet. Upload a CSV to get started.</p>
        )}

        {groups.map(({ label, runs }) => (
          <div key={label} className="mb-4">
            <p className="text-xs font-medium text-zinc-500 px-2 mb-1 uppercase tracking-wide">{label}</p>
            {runs.map((run) => {
              const isActive = run.runId === activeRunId
              const isSelected = run.runId === selectedRunId
              const dotColor = STATUS_DOT[run.status] ?? "bg-zinc-500"
              return (
                <button
                  key={run.runId}
                  onClick={() => onSelect(run.runId, run.fileId)}
                  className={`w-full text-left px-3 py-2.5 rounded-lg mb-0.5 transition-colors ${
                    isSelected ? "bg-zinc-700 text-white" : "text-zinc-300 hover:bg-zinc-800"
                  }`}
                >
                  <div className="flex items-center justify-between gap-2">
                    <div className="flex items-center gap-2 min-w-0">
                      <span className={`w-2 h-2 rounded-full flex-shrink-0 ${dotColor}`} />
                      <span className="text-xs font-mono font-medium truncate">
                        #{run.runId.slice(0, 8)}
                        {isActive && <span className="ml-1 text-blue-400">● live</span>}
                      </span>
                    </div>
                    <span className="text-xs text-zinc-500 flex-shrink-0">
                      {new Date(run.createdAt).toLocaleDateString("en-US", { month: "short", day: "numeric" })}
                    </span>
                  </div>
                  <p className="text-xs text-zinc-500 truncate mt-0.5 pl-4">{run.filename}</p>
                </button>
              )
            })}
          </div>
        ))}

        {/* Load more */}
        {hasMore && (
          <button
            onClick={() => setFileLimit((l) => l + PAGE_SIZE)}
            disabled={isFetching}
            className="w-full text-xs text-zinc-500 hover:text-zinc-300 py-2 transition-colors disabled:opacity-40"
          >
            {isFetching ? "Loading…" : "Load more"}
          </button>
        )}
      </div>
    </div>
  )
}
