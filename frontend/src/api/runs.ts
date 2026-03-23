import client from "./client"
import type { DataPreview, DownloadResponse, DropImpactRequest, DropImpactResponse } from "../types"

export async function getDownloadUrl(runId: string): Promise<DownloadResponse> {
  const res = await client.get<DownloadResponse>(`/runs/${runId}/download`)
  return res.data
}

export async function getDropImpact(runId: string, body: DropImpactRequest): Promise<DropImpactResponse> {
  const res = await client.post<DropImpactResponse>(`/runs/${runId}/drop-impact`, body)
  return res.data
}

export async function getRunPreview(runId: string, stage: "raw" | "cleaned" = "raw"): Promise<DataPreview> {
  const res = await client.get<DataPreview>(`/runs/${runId}/preview`, { params: { stage } })
  return res.data
}
