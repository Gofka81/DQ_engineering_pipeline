import { formatScore, scoreDelta } from "../../lib/formatters"

interface Props {
  label: string
  before: number
  after?: number
  prominent?: boolean
}

export function DQScoreBar({ label, before, after, prominent }: Props) {
  const pct = Math.min(100, Math.max(0, after ?? before))
  const beforePct = Math.min(100, Math.max(0, before))

  return (
    <div className={`flex items-center gap-3 ${prominent ? "mb-2" : ""}`}>
      <span className={`text-gray-600 dark:text-zinc-300 ${prominent ? "font-semibold w-28" : "text-sm w-28"}`}>
        {label}
      </span>
      <div className="flex-1 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden h-3">
        {after !== undefined ? (
          <div className="relative h-full">
            <div
              className="absolute h-full bg-gray-400 dark:bg-zinc-500 rounded-full"
              style={{ width: `${beforePct}%` }}
            />
            <div
              className="absolute h-full bg-green-500 rounded-full transition-all"
              style={{ width: `${pct}%` }}
            />
          </div>
        ) : (
          <div
            className="h-full bg-blue-500 rounded-full transition-all"
            style={{ width: `${pct}%` }}
          />
        )}
      </div>
      {after !== undefined ? (
        <span className={`text-sm font-medium w-24 text-right dark:text-zinc-200 ${prominent ? "text-base" : ""}`}>
          {formatScore(before)} → {formatScore(after)}{" "}
          <span className={after === before ? "text-gray-400 dark:text-zinc-500" : after > before ? "text-green-600" : "text-red-500"}>
            {scoreDelta(before, after)}
          </span>
        </span>
      ) : (
        <span className="text-sm text-gray-600 dark:text-zinc-300 w-16 text-right">{formatScore(before)}%</span>
      )}
    </div>
  )
}
