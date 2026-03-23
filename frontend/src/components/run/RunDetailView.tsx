import { useState, useRef, useCallback, useEffect } from "react"
import type { DataPreview, DQScores, Recommendations, RunOut, RunStatus } from "../../types"
import { submitRecommendations } from "../../api/files"

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
import { DataPreviewTable } from "./DataPreviewTable"
import { ScoreComparison } from "../scores/ScoreComparison"
import { RecommendationsEditor } from "../recommendations/RecommendationsEditor"
import { SubmitBar } from "../recommendations/SubmitBar"
import { EDADashboard } from "../eda/EDADashboard"
import { getDownloadUrl, getRunPreview } from "../../api/runs"

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
  const [activeTab, setActiveTab] = useState<"profile" | "recommendations" | "preview">("recommendations")
  const [completedTab, setCompletedTab] = useState<"recommendations" | "profile" | "preview">("recommendations")

  const [previewData, setPreviewData] = useState<DataPreview | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)

  useEffect(() => {
    setPreviewLoading(true)
    getRunPreview(runData.id)
      .then(setPreviewData)
      .catch(() => {})
      .finally(() => setPreviewLoading(false))
  }, [runData.id])

  const [cleanedPreview, setCleanedPreview] = useState<DataPreview | null>(null)

  useEffect(() => {
    if (status === "COMPLETED") {
      getRunPreview(runData.id, "cleaned").then(setCleanedPreview).catch(() => {})
    }
  }, [runData.id, status])

  const recsRef = useRef<Recommendations | null>(runData.recommendations_generated ?? null)
  const [submitLoading, setSubmitLoading] = useState(false)
  const [submitError, setSubmitError] = useState<string | null>(null)

  const handleRecsChange = useCallback((recs: Recommendations) => {
    recsRef.current = recs
  }, [])

  async function handleSubmit() {
    if (!recsRef.current) return
    setSubmitLoading(true)
    setSubmitError(null)
    try {
      await submitRecommendations(runData.file_id, recsRef.current)
      onRecommendationsSubmitted?.()
    } catch (err: unknown) {
      const msg =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        "Submit failed"
      setSubmitError(msg)
    } finally {
      setSubmitLoading(false)
    }
  }

  async function handleDownload() {
    const res = await getDownloadUrl(runData.id)
    window.open(res.download_url, "_blank")
  }

  return (
    <div className="flex flex-col flex-1 min-h-0">
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

        {/* AWAITING_REVIEW — tab switcher: Data Profile | Recommendations | Data Preview */}
        {status === "AWAITING_REVIEW" && (
          <>
            <div className="flex gap-1 bg-gray-100 dark:bg-zinc-800 rounded-lg p-1 w-fit">
              {(["recommendations", "profile", "preview"] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setActiveTab(tab)}
                  className={`px-4 py-1.5 text-sm rounded-md transition-colors ${
                    activeTab === tab
                      ? "bg-white dark:bg-zinc-700 text-gray-800 dark:text-zinc-100 shadow-sm font-medium"
                      : "text-gray-500 dark:text-zinc-400 hover:text-gray-700 dark:hover:text-zinc-300"
                  }`}
                >
                  {tab === "profile" ? "Data Profile" : tab === "recommendations" ? "Recommendations" : "Data Preview"}
                </button>
              ))}
            </div>

            {runData.recommendations_generated ? (
              <>
                {activeTab === "profile" && dqBefore && runData.recommendations_generated._eda && (
                  <EDADashboard
                    eda={runData.recommendations_generated._eda}
                    dqScores={dqBefore}
                    outliers={runData.recommendations_generated.outliers}
                  />
                )}
                {activeTab === "profile" && (!dqBefore || !runData.recommendations_generated._eda) && (
                  <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 text-center text-sm text-gray-400 dark:text-zinc-500">
                    Profile data not available — re-upload the file to generate it.
                  </div>
                )}
                {activeTab === "recommendations" && (
                  <RecommendationsEditor
                    runId={runData.id}
                    initialRecs={runData.recommendations_generated}
                    dqScoresBefore={dqBefore}
                    onRecsChange={handleRecsChange}
                  />
                )}
                {activeTab === "preview" && (
                  <DataPreviewTable
                    data={previewData}
                    loading={previewLoading}
                    totalRows={dqBefore?.total_rows ?? undefined}
                  />
                )}
              </>
            ) : (
              <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 flex items-center gap-3 text-blue-600">
                <svg className="w-5 h-5 animate-spin" fill="none" viewBox="0 0 24 24">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4l3-3-3-3v4a8 8 0 00-8 8h4z" />
                </svg>
                <span className="text-sm">Loading recommendations…</span>
              </div>
            )}
          </>
        )}

        {/* COMPLETED — tabbed view */}
        {status === "COMPLETED" && (
          <>
            <div className="flex gap-1 bg-gray-100 dark:bg-zinc-800 rounded-lg p-1 w-fit">
              {(["recommendations", "profile", "preview"] as const).map((tab) => (
                <button
                  key={tab}
                  onClick={() => setCompletedTab(tab)}
                  className={`px-4 py-1.5 text-sm rounded-md transition-colors ${
                    completedTab === tab
                      ? "bg-white dark:bg-zinc-700 text-gray-800 dark:text-zinc-100 shadow-sm font-medium"
                      : "text-gray-500 dark:text-zinc-400 hover:text-gray-700 dark:hover:text-zinc-300"
                  }`}
                >
                  {tab === "profile" ? "Data Profile" : tab === "recommendations" ? "Recommendations" : "Data Preview"}
                </button>
              ))}
            </div>

            {completedTab === "recommendations" && (
              (runData.recommendations_generated || runData.recommendations_approved) ? (
                <RecsViewer
                  generated={runData.recommendations_generated}
                  applied={runData.recommendations_approved}
                />
              ) : (
                <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 text-center text-sm text-gray-400 dark:text-zinc-500">
                  No recommendations available.
                </div>
              )
            )}

            {completedTab === "profile" && dqBefore && (
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
            )}
            {completedTab === "profile" && !dqBefore && (
              <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 text-center text-sm text-gray-400 dark:text-zinc-500">
                Profile data not available.
              </div>
            )}

            {completedTab === "preview" && (
              <DataPreviewTable
                data={cleanedPreview}
                loading={cleanedPreview === null}
                totalRows={dqAfter?.total_rows ?? dqBefore?.total_rows ?? undefined}
              />
            )}

            {onUploadNew && (
              <div className="flex gap-3">
                <button
                  onClick={onUploadNew}
                  className="px-4 py-2 text-sm border border-gray-300 dark:border-zinc-600 rounded-lg hover:bg-gray-50 dark:hover:bg-zinc-800 dark:text-zinc-300 transition-colors"
                >
                  Upload new file
                </button>
              </div>
            )}
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

        {/* Recommendations viewer — FAILED runs only */}
        {status === "FAILED" && (runData.recommendations_generated || runData.recommendations_approved) && (
          <div>
            <h2 className="text-sm font-semibold text-gray-600 dark:text-zinc-400 mb-2 uppercase tracking-wide">Recommendations</h2>
            <RecsViewer
              generated={runData.recommendations_generated}
              applied={runData.recommendations_approved}
            />
          </div>
        )}
      </div>

      {status === "AWAITING_REVIEW" && activeTab === "recommendations" && runData.recommendations_generated && (
        <>
          {submitError && <p className="text-red-500 text-sm px-6 py-1">{submitError}</p>}
          <SubmitBar onSubmit={handleSubmit} loading={submitLoading} />
        </>
      )}
    </div>
  )
}
