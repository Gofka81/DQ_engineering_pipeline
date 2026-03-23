import type { DQScores, EDAColumn, EDAColumnCategorical, EDAProfile, OutliersConfig } from "../../types"
import { DQRadarChart } from "../charts/DQRadarChart"
import { HistogramGrid } from "../charts/HistogramGrid"
import { BoxPlotGroup } from "../charts/BoxPlotGroup"

function MissingHeatmap({ columns }: { columns: EDAProfile["columns"] }) {
  const sorted = Object.entries(columns)
    .map(([name, col]) => ({ name, null_pct: col.null_pct }))
    .sort((a, b) => b.null_pct - a.null_pct)

  function tileColor(pct: number) {
    if (pct === 0) return "bg-green-50 dark:bg-green-950/30 border-green-200 dark:border-green-800/40"
    if (pct < 10) return "bg-yellow-50 dark:bg-yellow-950/30 border-yellow-200 dark:border-yellow-800/40"
    if (pct < 50) return "bg-amber-50 dark:bg-amber-950/40 border-amber-200 dark:border-amber-800/40"
    return "bg-red-50 dark:bg-red-950/40 border-red-200 dark:border-red-800/40"
  }

  function valueColor(pct: number) {
    if (pct === 0) return "text-green-600 dark:text-green-400"
    if (pct < 10) return "text-yellow-700 dark:text-yellow-400"
    if (pct < 50) return "text-amber-700 dark:text-amber-400"
    return "text-red-700 dark:text-red-400"
  }

  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-3">
        Missing Values
      </p>
      <div className="grid grid-cols-3 sm:grid-cols-4 gap-2">
        {sorted.map(({ name, null_pct }) => (
          <div key={name} className={`rounded-lg border px-2.5 py-2 ${tileColor(null_pct)}`}>
            <p className="text-xs font-medium text-gray-700 dark:text-zinc-300 truncate">{name}</p>
            <p className={`text-sm font-semibold tabular-nums mt-0.5 ${valueColor(null_pct)}`}>
              {null_pct === 0 ? "0%" : `${null_pct}%`}
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}

function isCategorical(col: EDAColumn): col is EDAColumnCategorical {
  return col.detected_type === "string" || col.detected_type === "bool"
}

function CategoricalBars({ columns }: { columns: EDAProfile["columns"] }) {
  const catCols = Object.entries(columns).filter(
    ([, col]) => isCategorical(col) && (col as EDAColumnCategorical).cardinality_pct < 10
  ) as [string, EDAColumnCategorical][]

  if (catCols.length === 0) return null

  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-3">
        Value Frequency — Categorical Columns
      </p>
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-3">
        {catCols.map(([colName, col]) => {
          const entries = Object.entries(col.top_values)
          if (entries.length === 0) return null
          const maxCount = Math.max(...entries.map(([, v]) => v))
          const total = entries.reduce((sum, [, v]) => sum + v, 0)
          return (
            <div
              key={colName}
              className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-4"
            >
              <div className="flex items-center justify-between mb-2">
                <p className="text-xs font-medium text-gray-700 dark:text-zinc-300 truncate">{colName}</p>
                {col.unique_count > 5 && (
                  <span className="text-xs text-gray-400 dark:text-zinc-500 flex-shrink-0 ml-2">
                    +{col.unique_count - 5} more
                  </span>
                )}
              </div>
              <div className="space-y-1.5">
                {entries.map(([value, count]) => {
                  const pct = total > 0 ? Math.round((count / total) * 1000) / 10 : 0
                  const barW = maxCount > 0 ? (count / maxCount) * 100 : 0
                  return (
                    <div key={value} className="flex items-center gap-2">
                      <div className="w-20 text-xs text-gray-500 dark:text-zinc-400 truncate text-right flex-shrink-0">
                        {value}
                      </div>
                      <div className="flex-1 bg-gray-100 dark:bg-zinc-800 rounded h-4 overflow-hidden">
                        <div
                          className="h-full bg-blue-400 dark:bg-blue-600 rounded"
                          style={{ width: `${barW}%` }}
                        />
                      </div>
                      <div className="w-12 text-xs text-gray-500 dark:text-zinc-400 tabular-nums text-right flex-shrink-0">
                        {pct}%
                      </div>
                    </div>
                  )
                })}
                {col.null_count > 0 && (
                  <div className="flex items-center gap-2">
                    <div className="w-20 text-xs text-gray-400 dark:text-zinc-500 text-right flex-shrink-0 italic">
                      null
                    </div>
                    <div className="flex-1 bg-gray-100 dark:bg-zinc-800 rounded h-4 overflow-hidden">
                      <div
                        className="h-full bg-amber-300 dark:bg-amber-700 rounded"
                        style={{ width: `${(col.null_count / (total + col.null_count)) * 100}%` }}
                      />
                    </div>
                    <div className="w-12 text-xs text-gray-400 dark:text-zinc-500 tabular-nums text-right flex-shrink-0">
                      {col.null_count}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )
        })}
      </div>
    </div>
  )
}

interface Props {
  eda: EDAProfile
  dqScores: DQScores
  outliers: Record<string, OutliersConfig>
}

export function EDADashboard({ eda, dqScores, outliers }: Props) {
  const pills = [
    { label: "Rows", value: eda.total_rows.toLocaleString() },
    { label: "Columns", value: String(eda.total_columns) },
    { label: "Duplicates", value: String(eda.duplicate_rows) },
    { label: "DQ Score", value: String(dqScores.overall) },
  ]

  return (
    <div className="flex flex-col gap-6">
      {/* Summary pills */}
      <div className="grid grid-cols-4 gap-3">
        {pills.map(({ label, value }) => (
          <div
            key={label}
            className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 px-4 py-3 text-center"
          >
            <div className="text-2xl font-semibold tabular-nums text-gray-800 dark:text-zinc-100">{value}</div>
            <div className="text-xs text-gray-400 dark:text-zinc-500 mt-0.5">{label}</div>
          </div>
        ))}
      </div>

      {/* DQ Radar + Missing heatmap */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4">
        <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-4">
          <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">
            DQ Score Breakdown
          </p>
          <DQRadarChart scores={dqScores} />
        </div>
        <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-4">
          <MissingHeatmap columns={eda.columns} />
        </div>
      </div>

      {/* Histograms */}
      <HistogramGrid columns={eda.columns} />

      {/* Box plots */}
      <BoxPlotGroup edaColumns={eda.columns} outliers={outliers} />

      {/* Categorical frequency */}
      <CategoricalBars columns={eda.columns} />
    </div>
  )
}
