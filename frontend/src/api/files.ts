import client from "./client"
import type { FileOut, FileUploadResponse, FileWithLatestRunOut, RunOut, RunStatusResponse } from "../types"

export async function uploadFile(
  file: File,
  onProgress?: (pct: number) => void,
  hasHeader = true
): Promise<FileUploadResponse> {
  const form = new FormData()
  form.append("file", file)
  form.append("has_header", String(hasHeader))
  const res = await client.post<FileUploadResponse>("/files/upload", form, {
    onUploadProgress: (e) => {
      if (onProgress && e.total) onProgress(Math.round((e.loaded / e.total) * 100))
    },
  })
  return res.data
}

export async function listFiles(skip = 0, limit = 50): Promise<FileOut[]> {
  const res = await client.get<FileOut[]>("/files", { params: { skip, limit } })
  return res.data
}

export async function listFilesWithLatestRun(skip = 0, limit = 50): Promise<FileWithLatestRunOut[]> {
  const res = await client.get<FileWithLatestRunOut[]>("/files/with-latest-run", { params: { skip, limit } })
  return res.data
}

export async function submitRecommendations(
  fileId: string,
  recommendations: unknown
): Promise<RunStatusResponse> {
  const res = await client.put<RunStatusResponse>(`/files/${fileId}/recommendations`, {
    recommendations,
  })
  return res.data
}

export async function listRuns(fileId: string): Promise<RunOut[]> {
  const res = await client.get<RunOut[]>(`/files/${fileId}/runs`)
  return res.data
}
