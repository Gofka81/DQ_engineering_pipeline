import { useState, useCallback, useEffect } from "react"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { AppShell } from "../components/layout/AppShell"
import { Sidebar } from "../components/sidebar/Sidebar"
import { UploadZone } from "../components/upload/UploadZone"
import { RunDetailView } from "../components/run/RunDetailView"
import { useRunEvents } from "../hooks/useRunEvents"
import { listFiles, listRuns } from "../api/files"
import type { DQScores, FileUploadResponse, RunOut, RunStatus, RunStatusEvent } from "../types"

interface LiveRunState {
  fileId: string
  runId: string
  filename: string
  status: RunStatus
  dqScoresBefore: DQScores | null
  dqScoresAfter: DQScores | null
  errorMessage: string | null
}

export function MainPage() {
  const [selectedRunId, setSelectedRunId] = useState<string | null>(() => {
    return sessionStorage.getItem("selectedRunId")
  })
  const [selectedFileId, setSelectedFileId] = useState<string | null>(() => {
    return sessionStorage.getItem("selectedFileId")
  })
  const [showUpload, setShowUpload] = useState(false)

  const [liveRun, setLiveRun] = useState<LiveRunState | null>(() => {
    const saved = sessionStorage.getItem("liveRun")
    return saved ? JSON.parse(saved) : null
  })

  const queryClient = useQueryClient()

  // Persist selection to sessionStorage
  useEffect(() => {
    if (selectedRunId) sessionStorage.setItem("selectedRunId", selectedRunId)
    else sessionStorage.removeItem("selectedRunId")
  }, [selectedRunId])

  useEffect(() => {
    if (selectedFileId) sessionStorage.setItem("selectedFileId", selectedFileId)
    else sessionStorage.removeItem("selectedFileId")
  }, [selectedFileId])

  useEffect(() => {
    if (liveRun) sessionStorage.setItem("liveRun", JSON.stringify(liveRun))
    else sessionStorage.removeItem("liveRun")
  }, [liveRun])

  // SSE for active run
  const handleSseUpdate = useCallback((event: RunStatusEvent) => {
    setLiveRun((prev) => {
      if (!prev) return prev
      return {
        ...prev,
        status: event.status,
        dqScoresBefore: event.dq_scores_before ?? prev.dqScoresBefore,
        dqScoresAfter: event.dq_scores_after ?? prev.dqScoresAfter,
        errorMessage: event.error_message ?? prev.errorMessage,
      }
    })
    queryClient.invalidateQueries({ queryKey: ["files-with-latest-run"] })
    if (event.status === "AWAITING_REVIEW" || event.status === "COMPLETED" || event.status === "FAILED") {
      if (liveRun?.fileId) {
        queryClient.invalidateQueries({ queryKey: ["runs", liveRun.fileId] })
      }
    }
    // Clear live tracking once terminal — removes "● live" badge and stops SSE
    if (event.status === "COMPLETED" || event.status === "FAILED") {
      setLiveRun(null)
    }
  }, [queryClient, liveRun?.fileId])

  useRunEvents(liveRun?.fileId ?? null, handleSseUpdate)

  // Load runs for the selected file
  const { data: selectedFileRuns = [], isLoading: runsLoading } = useQuery<RunOut[]>({
    queryKey: ["runs", selectedFileId],
    queryFn: () => listRuns(selectedFileId!),
    enabled: !!selectedFileId,
    staleTime: 15000,
    retry: false,
  })

  // Find the selected run's data
  const selectedRunData = selectedFileRuns.find((r) => r.id === selectedRunId) ?? null

  // Files list (for sidebar + filename lookup)
  const { data: files = [] } = useQuery({
    queryKey: ["files"],
    queryFn: () => listFiles(0, 100),
    staleTime: 30000,
  })

  function handleSelect(runId: string, fileId: string) {
    setSelectedRunId(runId)
    setSelectedFileId(fileId)
    setShowUpload(false)
  }

  function handleNewAnalysis() {
    setSelectedRunId(null)
    setSelectedFileId(null)
    setShowUpload(true)
  }

  function handleUploaded(res: FileUploadResponse) {
    const newLive: LiveRunState = {
      fileId: res.id,
      runId: res.run_id,
      filename: res.original_filename,
      status: res.status,
      dqScoresBefore: null,
      dqScoresAfter: null,
      errorMessage: null,
    }
    setLiveRun(newLive)
    setSelectedRunId(res.run_id)
    setSelectedFileId(res.id)
    setShowUpload(false)
    queryClient.invalidateQueries({ queryKey: ["files-with-latest-run"] })
  }

  function handleRecommendationsSubmitted() {
    setLiveRun((prev) => prev ? { ...prev, status: "TRANSFORMING" } : prev)
  }

  function handleUploadNew() {
    handleNewAnalysis()
  }

  function handleRestarted(newRunId: string, fileId: string) {
    setSelectedRunId(newRunId)
    setSelectedFileId(fileId)
    queryClient.invalidateQueries({ queryKey: ["runs", fileId] })
    queryClient.invalidateQueries({ queryKey: ["files-with-latest-run"] })
  }

  // Determine what to show in main
  const isActiveRun = selectedRunId === liveRun?.runId

  return (
    <AppShell
      sidebar={
        <Sidebar
          selectedRunId={selectedRunId}
          activeRunId={liveRun?.runId ?? null}
          activeStatus={liveRun?.status ?? null}
          onSelect={handleSelect}
          onNewAnalysis={handleNewAnalysis}
        />
      }
    >
      {/* Home/upload — no run selected, run not found after load, or "New Analysis" */}
      {(!selectedRunData || showUpload) && !runsLoading && (
        <div className="flex flex-col items-center justify-center min-h-full py-24">
          <h1 className="text-2xl font-semibold text-gray-700 dark:text-zinc-200 mb-2">Data Quality Pipeline</h1>
          <p className="text-gray-400 dark:text-zinc-500 mb-10 text-sm">Upload a CSV to start analysis</p>
          <UploadZone onUploaded={handleUploaded} />
        </div>
      )}

      {/* Selected run detail */}
      {selectedRunId && selectedRunData && !showUpload && (
        <RunDetailView
          runData={selectedRunData}
          filename={
            files.find((f) => f.id === selectedFileId)?.original_filename ??
            liveRun?.filename ??
            selectedRunData.file_id
          }
          liveStatus={isActiveRun ? liveRun?.status : undefined}
          liveDqScoresBefore={isActiveRun ? liveRun?.dqScoresBefore : undefined}
          liveDqScoresAfter={isActiveRun ? liveRun?.dqScoresAfter : undefined}
          liveErrorMessage={isActiveRun ? liveRun?.errorMessage : undefined}
          onRecommendationsSubmitted={isActiveRun ? handleRecommendationsSubmitted : undefined}
          onUploadNew={isActiveRun ? handleUploadNew : undefined}
          onRestarted={handleRestarted}
        />
      )}

      {/* Actively fetching a selected run */}
      {selectedRunId && !selectedRunData && runsLoading && !showUpload && (
        <div className="flex items-center justify-center min-h-full">
          <p className="text-gray-400 text-sm">Loading run details…</p>
        </div>
      )}
    </AppShell>
  )
}
