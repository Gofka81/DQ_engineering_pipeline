import { useState, useCallback, useEffect, useRef } from "react"
import type { ColumnConfig, DQScores, DropImpactResponse, DuplicatesConfig, OutliersConfig, Recommendations } from "../../types"
import { ColumnsTable } from "./ColumnsTable"
import { DuplicatesSection } from "./DuplicatesSection"
import { OutliersSection } from "./OutliersSection"
import { getDropImpact } from "../../api/runs"

interface Props {
  runId: string
  initialRecs: Recommendations
  dqScoresBefore: DQScores | null
  onRecsChange: (recs: Recommendations) => void
}

export function RecommendationsEditor({ runId, initialRecs, dqScoresBefore, onRecsChange }: Props) {
  const [recs, setRecs] = useState<Recommendations>(() => structuredClone(initialRecs))
  const [impact, setImpact] = useState<DropImpactResponse | null>(null)
  const onRecsChangeRef = useRef(onRecsChange)
  useEffect(() => { onRecsChangeRef.current = onRecsChange })
  useEffect(() => { onRecsChangeRef.current(recs) }, [recs])

  const updateColumn = useCallback((name: string, patch: Partial<ColumnConfig>) => {
    setRecs((prev) => ({
      ...prev,
      columns: {
        ...prev.columns,
        [name]: { ...prev.columns[name], ...patch },
      },
    }))
  }, [])

  const updateDuplicates = useCallback((patch: Partial<DuplicatesConfig>) => {
    setRecs((prev) => ({
      ...prev,
      duplicates: { ...(prev.duplicates ?? { strategy: "ignore", subset: [], keep: "first" }), ...patch },
    }))
  }, [])

  const updateOutlierStrategy = useCallback((col: string, strategy: OutliersConfig["strategy"]) => {
    setRecs((prev) => ({
      ...prev,
      outliers: {
        ...prev.outliers,
        [col]: { ...prev.outliers[col], strategy },
      },
    }))
  }, [])

  const clearAllRenames = useCallback(() => {
    setRecs((prev) => ({
      ...prev,
      columns: Object.fromEntries(
        Object.entries(prev.columns).map(([k, v]) => [k, { ...v, rename_to: null }])
      ),
    }))
  }, [])

  useEffect(() => {
    const timer = setTimeout(async () => {
      try {
        const result = await getDropImpact(runId, {
          null_strategies: Object.fromEntries(
            Object.entries(recs.columns)
              .filter(([, c]) => c.missing_values)
              .map(([k, c]) => [k, c.missing_values!.strategy])
          ),
          outlier_strategies: Object.fromEntries(
            Object.entries(recs.outliers ?? {}).map(([k, v]) => [k, v.strategy])
          ),
          duplicates_strategy: recs.duplicates?.strategy ?? "ignore",
        })
        setImpact(result)
      } catch {
        // silently ignore — feature is informational
      }
    }, 300)
    return () => clearTimeout(timer)
  }, [recs, runId])

  const issues = initialRecs._metadata?.issues_found ?? {}

  return (
    <div className="flex flex-col gap-4">
      {/* Stats banner */}
      <div className="flex flex-wrap items-center gap-2 text-xs">
        {dqScoresBefore && (
          <span className="bg-blue-100 dark:bg-blue-900/50 text-blue-700 dark:text-blue-300 rounded px-2 py-1 font-semibold">
            DQ {dqScoresBefore.overall.toFixed(1)}
          </span>
        )}
        {dqScoresBefore?.total_rows != null && (
          <span className="bg-gray-100 dark:bg-zinc-800 text-gray-600 dark:text-zinc-400 rounded px-2 py-1">
            {dqScoresBefore.total_rows.toLocaleString()} rows
          </span>
        )}
        {dqScoresBefore?.total_columns != null && (
          <span className="bg-gray-100 dark:bg-zinc-800 text-gray-600 dark:text-zinc-400 rounded px-2 py-1">
            {dqScoresBefore.total_columns} cols
          </span>
        )}
        {(issues.missing ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.missing} missing
          </span>
        )}
        {(issues.duplicates ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.duplicates} dupes
          </span>
        )}
        {(issues.type_mismatches ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.type_mismatches} type errors
          </span>
        )}
        {(issues.sentinel_values ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.sentinel_values} sentinels
          </span>
        )}
        {(issues.outliers ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.outliers} outliers
          </span>
        )}
        {(issues.format_inconsistencies ?? 0) > 0 && (
          <span className="bg-amber-100 dark:bg-amber-900/40 text-amber-700 dark:text-amber-300 rounded px-2 py-1">
            {issues.format_inconsistencies} format issues
          </span>
        )}
        {impact && (
          <span className="bg-emerald-100 dark:bg-emerald-900/40 text-emerald-700 dark:text-emerald-300 rounded px-2 py-1 font-semibold">
            Net: {impact.rows_after.toLocaleString()} / {impact.rows_before.toLocaleString()} rows
            {impact.breakdown.overlap_saved > 0 && (
              <span className="font-normal ml-1 opacity-75">
                ({impact.breakdown.overlap_saved} overlap saved)
              </span>
            )}
          </span>
        )}
      </div>

      <section>
        <h2 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-2">Columns</h2>
        <ColumnsTable columns={recs.columns} onChange={updateColumn} onClearAllRenames={clearAllRenames} />
      </section>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        <DuplicatesSection config={recs.duplicates} onChange={updateDuplicates} />
        <OutliersSection outliers={recs.outliers} onChange={updateOutlierStrategy} />
      </div>
    </div>
  )
}
