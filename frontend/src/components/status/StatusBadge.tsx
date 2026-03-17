import type { RunStatus } from "../../types"

const CONFIG: Record<RunStatus, { label: string; color: string; spin?: boolean }> = {
  PENDING: { label: "PENDING", color: "bg-gray-100 text-gray-600" },
  ANALYZING: { label: "ANALYZING", color: "bg-blue-100 text-blue-700", spin: true },
  AWAITING_REVIEW: { label: "AWAITING REVIEW", color: "bg-amber-100 text-amber-700" },
  TRANSFORMING: { label: "TRANSFORMING", color: "bg-blue-100 text-blue-700", spin: true },
  COMPLETED: { label: "COMPLETED", color: "bg-green-100 text-green-700" },
  FAILED: { label: "FAILED", color: "bg-red-100 text-red-700" },
}

export function StatusBadge({ status }: { status: RunStatus }) {
  const { label, color, spin } = CONFIG[status] ?? CONFIG.PENDING
  return (
    <span className={`inline-flex items-center gap-1.5 px-2.5 py-1 rounded-full text-xs font-medium ${color}`}>
      {spin ? (
        <svg className="w-3 h-3 animate-spin" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4l3-3-3-3v4a8 8 0 00-8 8h4z" />
        </svg>
      ) : (
        <span className="w-1.5 h-1.5 rounded-full bg-current" />
      )}
      {label}
    </span>
  )
}
