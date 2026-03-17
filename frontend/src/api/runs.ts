import client from "./client"
import type { DownloadResponse } from "../types"

export async function getDownloadUrl(runId: string): Promise<DownloadResponse> {
  const res = await client.get<DownloadResponse>(`/runs/${runId}/download`)
  return res.data
}
