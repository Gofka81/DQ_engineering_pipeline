import { useCallback, useState } from "react"
import { uploadFile } from "../../api/files"
import type { FileUploadResponse } from "../../types"

interface Props {
  onUploaded: (res: FileUploadResponse) => void
}

export function UploadZone({ onUploaded }: Props) {
  const [dragging, setDragging] = useState(false)
  const [progress, setProgress] = useState<number | null>(null)
  const [error, setError] = useState<string | null>(null)

  const handleFile = useCallback(async (file: File) => {
    if (!file.name.toLowerCase().endsWith(".csv")) {
      setError("Only CSV files are allowed")
      return
    }
    if (file.size > 200 * 1024 * 1024) {
      setError("File exceeds 200MB limit")
      return
    }
    setError(null)
    setProgress(0)
    try {
      const res = await uploadFile(file, setProgress)
      onUploaded(res)
    } catch (err: unknown) {
      const msg =
        (err as { response?: { data?: { detail?: string } } })?.response?.data?.detail ||
        "Upload failed"
      setError(msg)
    } finally {
      setProgress(null)
    }
  }, [onUploaded])

  function onDrop(e: React.DragEvent) {
    e.preventDefault()
    setDragging(false)
    const file = e.dataTransfer.files[0]
    if (file) handleFile(file)
  }

  function onInputChange(e: React.ChangeEvent<HTMLInputElement>) {
    const file = e.target.files?.[0]
    if (file) handleFile(file)
    e.target.value = ""
  }

  return (
    <div className="flex flex-col items-center gap-4">
      <label
        onDragOver={(e) => { e.preventDefault(); setDragging(true) }}
        onDragLeave={() => setDragging(false)}
        onDrop={onDrop}
        className={`w-full max-w-lg border-2 border-dashed rounded-xl p-12 flex flex-col items-center gap-3 cursor-pointer transition-colors ${
          dragging
            ? "border-blue-400 bg-blue-50 dark:bg-blue-950/30"
            : "border-gray-300 dark:border-zinc-600 hover:border-gray-400 dark:hover:border-zinc-500 bg-white dark:bg-zinc-900"
        }`}
      >
        <svg className="w-10 h-10 text-gray-400 dark:text-zinc-500" fill="none" stroke="currentColor" viewBox="0 0 24 24">
          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M3 16.5v2.25A2.25 2.25 0 005.25 21h13.5A2.25 2.25 0 0021 18.75V16.5m-13.5-9L12 3m0 0l4.5 4.5M12 3v13.5" />
        </svg>
        <p className="text-gray-600 dark:text-zinc-300 text-sm">Drop CSV here or click to browse</p>
        <p className="text-gray-400 dark:text-zinc-500 text-xs">Max 200MB</p>
        <input type="file" accept=".csv" className="hidden" onChange={onInputChange} />
      </label>

      {progress !== null && (
        <div className="w-full max-w-lg">
          <div className="h-2 bg-gray-200 dark:bg-zinc-700 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 transition-all"
              style={{ width: `${progress}%` }}
            />
          </div>
          <p className="text-xs text-gray-500 dark:text-zinc-400 mt-1 text-center">{progress}%</p>
        </div>
      )}

      {error && <p className="text-red-500 text-sm">{error}</p>}
    </div>
  )
}
