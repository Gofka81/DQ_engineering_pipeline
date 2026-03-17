export type RunStatus =
  | "PENDING"
  | "ANALYZING"
  | "AWAITING_REVIEW"
  | "TRANSFORMING"
  | "COMPLETED"
  | "FAILED"

export interface MissingValuesFill {
  strategy: "median" | "mean" | "mode" | "fill" | "drop_row" | "drop_column" | "leave_null"
  value?: string | number | null
}

export interface ColumnConfig {
  type: "int" | "float" | "string" | "date" | "bool"
  nullable: boolean
  missing_values: MissingValuesFill | null
  normalize: "min_max" | "z_score" | false
  warnings: string[]
  note: string | null
  rename_to: string | null
  sentinel_values: number[] | null
  transform_hint?: string | null
  transform_code?: string | null
}

export interface DuplicatesConfig {
  strategy: "drop" | "ignore"
  subset: string[]
  keep: "first" | "last"
}

export interface OutliersConfig {
  strategy: "keep" | "winsorise" | "remove" | "cap"
  method: "iqr"
  lower: number | null
  upper: number | null
  count?: number
}

export interface Recommendations {
  columns: Record<string, ColumnConfig>
  duplicates: DuplicatesConfig | null
  outliers: Record<string, OutliersConfig>
  custom_transforms: Record<string, unknown>[]
  _metadata: {
    generated_at: string
    dq_score: number
    issues_found: Record<string, number>
  }
}

export interface DQScores {
  overall: number
  completeness: number
  uniqueness: number
  validity: number
  consistency: number
  issues_found?: Record<string, number>
  total_rows?: number
  total_columns?: number
  malformed_rows?: number
}

export interface FileOut {
  id: string
  original_filename: string
  file_size: number
  uploaded_at: string
}

export interface FileWithLatestRunOut {
  id: string
  original_filename: string
  file_size: number
  uploaded_at: string
  run_id: string | null
  run_status: RunStatus | null
  run_created_at: string | null
}

export interface RunOut {
  id: string
  file_id: string
  status: RunStatus
  dq_scores_before: DQScores | null
  dq_scores_after: DQScores | null
  recommendations_generated: Recommendations | null
  recommendations_approved: Recommendations | null
  created_at: string
  completed_at: string | null
  error_message: string | null
  storage_expired: boolean
}

export interface RunStatusResponse {
  run_id: string
  file_id: string
  status: RunStatus
  dq_scores_before: DQScores | null
  dq_scores_after: DQScores | null
  error_message: string | null
}

export interface RunStatusEvent {
  status: RunStatus
  dq_scores_before?: DQScores | null
  dq_scores_after?: DQScores | null
  error_message?: string | null
}

export interface FileUploadResponse {
  id: string
  original_filename: string
  file_size: number
  uploaded_at: string
  run_id: string
  status: RunStatus
}

export interface RecommendationsOut {
  run_id: string
  file_id: string
  status: RunStatus
  recommendations: Recommendations | null
}

export interface DownloadResponse {
  run_id: string
  download_url: string
  expires_in_hours: number
}
