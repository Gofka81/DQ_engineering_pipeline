import type { DQScores, RunOut, RunStatus } from "../../types"

const ISSUE_LABELS: Record<string, string> = {
  missing: "Missing values",
  duplicates: "Duplicates",
  type_mismatches: "Type mismatches",
  invalid_values: "Invalid values",
  sentinel_values: "Sentinels",
  format_inconsistencies: "Format issues",
  outliers: "Outliers",
  malformed_rows: "Malformed rows",
}

const STAT_LABELS: Record<string, string> = {
  total_rows: "Rows",
  total_columns: "Columns",
}

function IssuesGrid({
  before,
  after,
  statsBefore,
  statsAfter,
}: {
  before: Record<string, number>
  after?: Record<string, number>
  statsBefore?: DQScores
  statsAfter?: DQScores
}) {
  const entries = Object.entries(before).filter(([k]) => k in ISSUE_LABELS)
  const statEntries = Object.entries(STAT_LABELS).filter(
    ([k]) => statsBefore?.[k as keyof DQScores] != null
  )
  if (entries.length === 0 && statEntries.length === 0) return null
  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-3">
        Issues detected
      </p>
      <div className="grid grid-cols-2 sm:grid-cols-3 gap-2">
        {statEntries.map(([key, label]) => {
          const beforeVal = statsBefore?.[key as keyof DQScores] as number
          const afterVal = (statsAfter?.[key as keyof DQScores] as number) ?? beforeVal
          const delta = afterVal - beforeVal
          return (
            <div key={key} className="rounded-lg px-3 py-2.5 flex flex-col gap-0.5 bg-gray-50 dark:bg-zinc-800">
              <div className="flex items-baseline gap-1 flex-wrap">
                <span className="text-xl font-semibold tabular-nums text-gray-700 dark:text-zinc-200">
                  {afterVal.toLocaleString()}
                </span>
                {delta !== 0 && (
                  <>
                    <span className="text-xs text-gray-400 dark:text-zinc-500">← {beforeVal.toLocaleString()}</span>
                    <span className={`text-xs font-medium tabular-nums ${delta < 0 ? "text-red-500" : "text-green-600 dark:text-green-400"}`}>
                      {delta > 0 ? "+" : ""}{delta.toLocaleString()}
                    </span>
                  </>
                )}
              </div>
              <span className="text-xs text-gray-500 dark:text-zinc-400">{label}</span>
            </div>
          )
        })}
        {entries.map(([key, beforeCount]) => {
          const afterCount = after?.[key] ?? 0
          const delta = afterCount - beforeCount
          return (
            <div
              key={key} 
              className={`rounded-lg px-3 py-2.5 flex flex-col gap-0.5 ${
                beforeCount === 0
                  ? "bg-gray-50 dark:bg-zinc-800"
                  : "bg-amber-50 dark:bg-amber-950/40"
              }`}
            >
              <div className="flex items-baseline gap-1 flex-wrap">
                <span className={`text-xl font-semibold tabular-nums ${
                  beforeCount === 0 ? "text-gray-400 dark:text-zinc-500" : afterCount === 0 ? "text-green-600 dark:text-green-400" : "text-amber-700 dark:text-amber-300"
                }`}>
                  {afterCount.toLocaleString()}
                </span>
                {beforeCount > 0 && (
                  <>
                    <span className="text-xs text-gray-400 dark:text-zinc-500">← {beforeCount.toLocaleString()}</span>
                    {delta !== 0 && (
                      <span className={`text-xs font-medium tabular-nums ${delta < 0 ? "text-green-600 dark:text-green-400" : "text-red-500"}`}>
                        {delta > 0 ? "+" : ""}{delta.toLocaleString()}
                      </span>
                    )}
                  </>
                )}
              </div>
              <span className="text-xs text-gray-500 dark:text-zinc-400">
                {ISSUE_LABELS[key]}
              </span>
            </div>
          )
        })}
      </div>
    </div>
  )
}
import { RunHeader } from "./RunHeader"
import { RecsViewer } from "./RecsViewer"
import { ScoreComparison } from "../scores/ScoreComparison"
import { RecommendationsEditor } from "../recommendations/RecommendationsEditor"
import { getDownloadUrl } from "../../api/runs"

interface Props {
  runData: RunOut
  filename: string
  // Live overrides for the active SSE run
  liveStatus?: RunStatus
  liveDqScoresBefore?: DQScores | null
  liveDqScoresAfter?: DQScores | null
  liveErrorMessage?: string | null
  onRecommendationsSubmitted?: () => void
  onUploadNew?: () => void
}

export function RunDetailView({
  runData,
  filename,
  liveStatus,
  liveDqScoresBefore,
  liveDqScoresAfter,
  liveErrorMessage,
  onRecommendationsSubmitted,
  onUploadNew,
}: Props) {
  const status = liveStatus ?? runData.status
  const dqBefore = liveDqScoresBefore !== undefined ? liveDqScoresBefore : runData.dq_scores_before
  const dqAfter = liveDqScoresAfter !== undefined ? liveDqScoresAfter : runData.dq_scores_after
  const errorMessage = liveErrorMessage !== undefined ? liveErrorMessage : runData.error_message

  async function handleDownload() {
    const res = await getDownloadUrl(runData.id)
    window.open(res.download_url, "_blank")
  }

  return (
    <div className="flex flex-col h-full">
      <RunHeader
        runId={runData.id}
        filename={filename}
        status={status}
        createdAt={runData.created_at}
        storageExpired={runData.storage_expired}
        onDownload={status === "COMPLETED" ? handleDownload : undefined}
      />

      <div className="flex-1 overflow-y-auto p-6 flex flex-col gap-6">

        {/* PENDING / ANALYZING / TRANSFORMING — spinner */}
        {(status === "PENDING" || status === "ANALYZING" || status === "TRANSFORMING") && (
          <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-10 flex flex-col items-center gap-4 text-center">
            <svg className="w-8 h-8 animate-spin text-blue-500" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4l3-3-3-3v4a8 8 0 00-8 8h4z" />
            </svg>
            <div>
              <p className="text-gray-700 dark:text-zinc-200 font-medium">
                {status === "PENDING" && "Queued for analysis…"}
                {status === "ANALYZING" && "Profiling columns and generating recommendations…"}
                {status === "TRANSFORMING" && "Applying transformations…"}
              </p>
              <p className="text-gray-400 dark:text-zinc-500 text-sm mt-1">Updates arrive in real time</p>
            </div>
          </div>
        )}

        {/* AWAITING_REVIEW — interactive editor */}
        {status === "AWAITING_REVIEW" && runData.recommendations_generated && (
          <RecommendationsEditor
            fileId={runData.file_id}
            initialRecs={runData.recommendations_generated}
            dqScoresBefore={dqBefore}
            onSubmitted={onRecommendationsSubmitted ?? (() => {})}
          />
        )}
        {status === "AWAITING_REVIEW" && !runData.recommendations_generated && (
          <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 flex items-center gap-3 text-blue-600">
            <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4l3-3-3-3v4a8 8 0 00-8 8h4z" />
            </svg>
            <span className="text-sm">Loading recommendations…</span>
          </div>
        )}

        {/* COMPLETED — scores + issues */}
        {status === "COMPLETED" && dqBefore && (
          <>
            <h2 className="text-sm font-semibold text-gray-600 dark:text-zinc-400 uppercase tracking-wide -mb-2">Statistics</h2>
            <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-6 flex gap-6">
              <div className="flex-1 min-w-0">
                <ScoreComparison before={dqBefore} after={dqAfter ?? undefined} />
              </div>
              {dqBefore?.issues_found && (
                <div className="w-px bg-gray-100 dark:bg-zinc-700 flex-shrink-0" />
              )}
              {dqBefore?.issues_found && (
                <div className="flex-1 min-w-0">
                  <IssuesGrid
                    before={dqBefore.issues_found}
                    after={dqAfter?.issues_found}
                    statsBefore={dqBefore}
                    statsAfter={dqAfter ?? undefined}
                  />
                </div>
              )}
            </div>
            <div className="flex gap-3">
              {onUploadNew && (
                <button
                  onClick={onUploadNew}
                  className="px-4 py-2 text-sm border border-gray-300 dark:border-zinc-600 rounded-lg hover:bg-gray-50 dark:hover:bg-zinc-800 dark:text-zinc-300 transition-colors"
                >
                  Upload new file
                </button>
              )}
            </div>
          </>
        )}

        {/* FAILED */}
        {status === "FAILED" && (
          <div className="bg-red-50 dark:bg-red-950 border border-red-200 dark:border-red-800 rounded-xl p-6">
            <p className="font-medium text-red-700 dark:text-red-400 mb-1">Analysis failed</p>
            {errorMessage && <p className="text-sm text-red-500 dark:text-red-400">{errorMessage}</p>}
            {onUploadNew && (
              <button
                onClick={onUploadNew}
                className="mt-3 px-4 py-2 text-sm border border-red-300 dark:border-red-700 text-red-700 dark:text-red-400 rounded-lg hover:bg-red-100 dark:hover:bg-red-900"
              >
                Upload new file
              </button>
            )}
          </div>
        )}

        {/* Recommendations viewer — always show for completed or past runs */}
        {(status === "COMPLETED" || (status !== "PENDING" && status !== "ANALYZING" && status !== "AWAITING_REVIEW" && status !== "TRANSFORMING")) && (
          (runData.recommendations_generated || runData.recommendations_approved) && (
            <div>
              <h2 className="text-sm font-semibold text-gray-600 dark:text-zinc-400 mb-2 uppercase tracking-wide">Recommendations</h2>
              <RecsViewer
                generated={runData.recommendations_generated}
                applied={runData.recommendations_approved}
              />
            </div>
          )
        )}
      </div>
    </div>
  )
}
