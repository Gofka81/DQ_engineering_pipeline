import { useState } from "react"
import type { ColumnConfig, DuplicatesConfig, OutliersConfig, Recommendations } from "../../types"

interface Props {
  generated: Recommendations | null
  applied: Recommendations | null
}

// ---------------------------------------------------------------------------
// Diff types + computation
// ---------------------------------------------------------------------------

interface FieldDiff {
  field: string
  from: string
  to: string
}

interface ColumnDiff {
  name: string
  changes: FieldDiff[]
}

interface RecsDiff {
  columns: ColumnDiff[]
  duplicates: FieldDiff[]
  outliers: { column: string; from: string; to: string }[]
  totalChanges: number
}

function serializeFill(mv: ColumnConfig["missing_values"]): string {
  if (!mv) return "—"
  return mv.strategy === "fill" && mv.value != null
    ? `${mv.strategy} (${mv.value})`
    : mv.strategy
}

function serializeNormalize(n: ColumnConfig["normalize"]): string {
  return n === false ? "none" : n
}

function serializeDuplicates(d: DuplicatesConfig | null): Record<string, string> {
  if (!d) return { strategy: "—" }
  return {
    strategy: d.strategy,
    keep: d.strategy === "drop" ? d.keep : "—",
    subset: d.subset?.length ? d.subset.join(", ") : "—",
  }
}

function computeDiff(gen: Recommendations, app: Recommendations): RecsDiff {
  const columnDiffs: ColumnDiff[] = []

  const allCols = new Set([...Object.keys(gen.columns), ...Object.keys(app.columns)])

  for (const name of allCols) {
    const g = gen.columns[name]
    const a = app.columns[name]
    const changes: FieldDiff[] = []

    if (!g || !a) {
      changes.push({ field: "column", from: g ? "present" : "—", to: a ? "present" : "removed" })
    } else {
      const checks: [string, string, string][] = [
        ["type", g.type, a.type],
        ["fill", serializeFill(g.missing_values), serializeFill(a.missing_values)],
        ["normalize", serializeNormalize(g.normalize), serializeNormalize(a.normalize)],
        ["rename_to", g.rename_to ?? "—", a.rename_to ?? "—"],
        ["nullable", g.nullable ? "yes" : "no", a.nullable ? "yes" : "no"],
        ["transform_hint", g.transform_hint ?? "—", a.transform_hint ?? "—"],
        ["transform_code", g.transform_code ?? "—", a.transform_code ?? "—"],
      ]
      for (const [field, from, to] of checks) {
        if (from !== to) changes.push({ field, from, to })
      }
    }

    if (changes.length > 0) columnDiffs.push({ name, changes })
  }

  // Duplicates diff
  const dupDiffs: FieldDiff[] = []
  const gDup = serializeDuplicates(gen.duplicates)
  const aDup = serializeDuplicates(app.duplicates)
  for (const key of Object.keys(gDup)) {
    if (gDup[key] !== (aDup[key] ?? "—")) {
      dupDiffs.push({ field: key, from: gDup[key], to: aDup[key] ?? "—" })
    }
  }

  // Outliers diff (strategy only — bounds are system-set)
  const outlierDiffs: { column: string; from: string; to: string }[] = []
  const allOutlierCols = new Set([
    ...Object.keys(gen.outliers ?? {}),
    ...Object.keys(app.outliers ?? {}),
  ])
  for (const col of allOutlierCols) {
    const gS = (gen.outliers?.[col] as OutliersConfig | undefined)?.strategy ?? "—"
    const aS = (app.outliers?.[col] as OutliersConfig | undefined)?.strategy ?? "—"
    if (gS !== aS) outlierDiffs.push({ column: col, from: gS, to: aS })
  }

  const totalChanges = columnDiffs.length + (dupDiffs.length > 0 ? 1 : 0) + outlierDiffs.length

  return { columns: columnDiffs, duplicates: dupDiffs, outliers: outlierDiffs, totalChanges }
}

// ---------------------------------------------------------------------------
// Main component
// ---------------------------------------------------------------------------

export function RecsViewer({ generated, applied }: Props) {
  const [tab, setTab] = useState<"generated" | "applied" | "diff">("generated")
  const recs = tab === "generated" ? generated : tab === "applied" ? applied : null
  const hasApplied = !!applied

  const diff = generated && applied ? computeDiff(generated, applied) : null

  return (
    <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700">
      {/* Tab bar */}
      <div className="flex border-b border-gray-200 dark:border-zinc-700 px-4 pt-3 gap-1">
        <TabBtn active={tab === "generated"} onClick={() => setTab("generated")}>
          Generated
        </TabBtn>
        <TabBtn active={tab === "applied"} onClick={() => setTab("applied")} disabled={!hasApplied}>
          Applied {!hasApplied && <span className="text-xs text-gray-400 dark:text-zinc-500 ml-1">(none)</span>}
        </TabBtn>
        <TabBtn active={tab === "diff"} onClick={() => setTab("diff")} disabled={!hasApplied}>
          Diff
          {diff && diff.totalChanges > 0 && (
            <span className="ml-1.5 px-1.5 py-0.5 rounded-full bg-amber-100 dark:bg-amber-900/50 text-amber-700 dark:text-amber-300 text-xs font-semibold">
              {diff.totalChanges}
            </span>
          )}
        </TabBtn>
      </div>

      {/* Diff tab */}
      {tab === "diff" && diff && (
        <div className="p-4 flex flex-col gap-6">
          {diff.totalChanges === 0 ? (
            <p className="text-sm text-gray-500 dark:text-zinc-400 py-4 text-center">
              No changes — applied recommendations are identical to generated.
            </p>
          ) : (
            <>
              <p className="text-xs text-gray-400 dark:text-zinc-500">
                {diff.columns.length} column{diff.columns.length !== 1 ? "s" : ""} changed
                {diff.duplicates.length > 0 && " · duplicates changed"}
                {diff.outliers.length > 0 && ` · ${diff.outliers.length} outlier strateg${diff.outliers.length !== 1 ? "ies" : "y"} changed`}
              </p>

              {/* Column diffs */}
              {diff.columns.length > 0 && (
                <section>
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Columns</h3>
                  <div className="divide-y divide-gray-100 dark:divide-zinc-700 border border-gray-200 dark:border-zinc-700 rounded-lg overflow-hidden">
                    {diff.columns.map(({ name, changes }) => (
                      <div key={name} className="px-4 py-3 dark:bg-zinc-900">
                        <p className="font-mono text-sm font-medium text-gray-800 dark:text-zinc-200 mb-2">{name}</p>
                        <div className="flex flex-col gap-1">
                          {changes.map(({ field, from, to }) => (
                            <div key={field} className="flex items-baseline gap-2 text-xs">
                              <span className="text-gray-400 dark:text-zinc-500 w-28 flex-shrink-0">{field}</span>
                              <span className="line-through text-red-400 font-mono">{from}</span>
                              <span className="text-gray-400 dark:text-zinc-500">→</span>
                              <span className="text-green-600 dark:text-green-400 font-mono font-medium">{to}</span>
                            </div>
                          ))}
                        </div>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {/* Duplicates diff */}
              {diff.duplicates.length > 0 && (
                <section>
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Duplicates</h3>
                  <div className="border border-gray-200 dark:border-zinc-700 rounded-lg px-4 py-3 flex flex-col gap-1 dark:bg-zinc-900">
                    {diff.duplicates.map(({ field, from, to }) => (
                      <div key={field} className="flex items-baseline gap-2 text-xs">
                        <span className="text-gray-400 dark:text-zinc-500 w-28 flex-shrink-0">{field}</span>
                        <span className="line-through text-red-400 font-mono">{from}</span>
                        <span className="text-gray-400 dark:text-zinc-500">→</span>
                        <span className="text-green-600 dark:text-green-400 font-mono font-medium">{to}</span>
                      </div>
                    ))}
                  </div>
                </section>
              )}

              {/* Outliers diff */}
              {diff.outliers.length > 0 && (
                <section>
                  <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Outlier strategies</h3>
                  <div className="divide-y divide-gray-100 dark:divide-zinc-700 border border-gray-200 dark:border-zinc-700 rounded-lg overflow-hidden">
                    {diff.outliers.map(({ column, from, to }) => (
                      <div key={column} className="px-4 py-2.5 flex items-center gap-4 text-xs dark:bg-zinc-900">
                        <span className="font-mono text-gray-800 dark:text-zinc-200 flex-1">{column}</span>
                        <span className="line-through text-red-400 font-mono">{from}</span>
                        <span className="text-gray-400 dark:text-zinc-500">→</span>
                        <span className="text-green-600 dark:text-green-400 font-mono font-medium">{to}</span>
                      </div>
                    ))}
                  </div>
                </section>
              )}
            </>
          )}
        </div>
      )}

      {/* Generated / Applied tab content */}
      {tab !== "diff" && recs && (
        <div className="p-4 flex flex-col gap-6">
          {recs._metadata && (
            <div className="flex flex-wrap gap-4 text-sm">
              <span className="text-gray-600 dark:text-zinc-300">
                DQ Score: <strong className="text-gray-800 dark:text-zinc-100">{recs._metadata.dq_score?.toFixed(1)}</strong>
              </span>
              {Object.entries(recs._metadata.issues_found ?? {}).filter(([, v]) => v > 0).map(([k, v]) => (
                <span key={k} className="text-amber-600 dark:text-amber-400">{v} {k.replace(/_/g, " ")}</span>
              ))}
            </div>
          )}

          <section>
            <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Columns</h3>
            <div className="divide-y divide-gray-100 dark:divide-zinc-700 border border-gray-200 dark:border-zinc-700 rounded-lg overflow-hidden">
              {Object.entries(recs.columns).map(([name, cfg]) => (
                <ColumnAccordion key={name} name={name} cfg={cfg} />
              ))}
            </div>
          </section>

          {recs.duplicates && (
            <section>
              <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Duplicates</h3>
              <div className="text-sm text-gray-700 dark:text-zinc-300 bg-gray-50 dark:bg-zinc-800 rounded-lg px-4 py-3 flex gap-6">
                <span>Strategy: <strong>{recs.duplicates.strategy}</strong></span>
                {recs.duplicates.strategy === "drop" && (
                  <span>Keep: <strong>{recs.duplicates.keep}</strong></span>
                )}
                {recs.duplicates.subset?.length > 0 && (
                  <span>Subset: <strong>{recs.duplicates.subset.join(", ")}</strong></span>
                )}
              </div>
            </section>
          )}

          {Object.keys(recs.outliers).length > 0 && (
            <section>
              <h3 className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-2">Outliers</h3>
              <div className="border border-gray-200 dark:border-zinc-700 rounded-lg overflow-hidden">
                <table className="w-full text-sm">
                  <thead className="bg-gray-50 dark:bg-zinc-800 border-b border-gray-200 dark:border-zinc-700">
                    <tr>
                      <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 dark:text-zinc-400">Column</th>
                      <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 dark:text-zinc-400">Count</th>
                      <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 dark:text-zinc-400">IQR Bounds</th>
                      <th className="px-3 py-2 text-left text-xs font-semibold text-gray-500 dark:text-zinc-400">Strategy</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-gray-100 dark:divide-zinc-700">
                    {Object.entries(recs.outliers).map(([col, cfg]) => (
                      <tr key={col} className="dark:bg-zinc-900">
                        <td className="px-3 py-2 font-mono text-gray-800 dark:text-zinc-200">{col}</td>
                        <td className="px-3 py-2 text-gray-600 dark:text-zinc-400">{cfg.count ?? "—"}</td>
                        <td className="px-3 py-2 text-gray-600 dark:text-zinc-400">
                          {cfg.lower != null && cfg.upper != null
                            ? `${cfg.lower.toLocaleString()} – ${cfg.upper.toLocaleString()}`
                            : "—"}
                        </td>
                        <td className="px-3 py-2">
                          <span className="px-2 py-0.5 rounded bg-gray-100 dark:bg-zinc-700 text-gray-700 dark:text-zinc-300 text-xs font-medium">
                            {cfg.strategy}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            </section>
          )}
        </div>
      )}

      {tab !== "diff" && !recs && (
        <p className="text-sm text-gray-400 dark:text-zinc-500 p-6">No recommendations data available.</p>
      )}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Sub-components
// ---------------------------------------------------------------------------

function TabBtn({
  active, onClick, disabled, children,
}: {
  active: boolean
  onClick: () => void
  disabled?: boolean
  children: React.ReactNode
}) {
  return (
    <button
      onClick={onClick}
      disabled={disabled}
      className={`px-4 py-2 text-sm font-medium border-b-2 -mb-px transition-colors disabled:opacity-40 flex items-center gap-1 ${
        active
          ? "border-blue-500 text-blue-600 dark:text-blue-400"
          : "border-transparent text-gray-500 dark:text-zinc-400 hover:text-gray-700 dark:hover:text-zinc-200"
      }`}
    >
      {children}
    </button>
  )
}

function ColumnAccordion({ name, cfg }: { name: string; cfg: ColumnConfig }) {
  const [open, setOpen] = useState(false)
  const hasWarnings = cfg.warnings.length > 0
  const hasTransform = !!(cfg.transform_hint || cfg.transform_code)
  const hasSentinels = !!(cfg.sentinel_values?.length)

  return (
    <div className={hasWarnings ? "bg-amber-50 dark:bg-amber-950/30" : "dark:bg-zinc-900"}>
      <button
        onClick={() => setOpen((o) => !o)}
        className="w-full flex items-center gap-3 px-4 py-3 text-left hover:bg-gray-50 dark:hover:bg-zinc-800 transition-colors"
      >
        <span className="text-gray-400 dark:text-zinc-500 text-xs w-3">{open ? "▼" : "▶"}</span>
        <span className="font-mono text-sm font-medium text-gray-800 dark:text-zinc-200 flex-1">{name}</span>
        {cfg.rename_to && <span className="text-xs text-blue-500 font-mono">→ {cfg.rename_to}</span>}
        <span className="text-xs bg-gray-100 dark:bg-zinc-700 text-gray-600 dark:text-zinc-300 px-2 py-0.5 rounded font-mono">{cfg.type}</span>
        <div className="flex gap-1">
          {hasWarnings && <span className="text-xs bg-amber-100 dark:bg-amber-900/50 text-amber-700 dark:text-amber-300 px-1.5 py-0.5 rounded">⚠ {cfg.warnings.length}</span>}
          {hasTransform && <span className="text-xs bg-blue-50 dark:bg-blue-950 text-blue-600 dark:text-blue-400 px-1.5 py-0.5 rounded">→ transform</span>}
          {hasSentinels && <span className="text-xs bg-red-50 dark:bg-red-950 text-red-600 dark:text-red-400 px-1.5 py-0.5 rounded">sentinel</span>}
        </div>
      </button>

      {open && (
        <div className="px-6 pb-4 text-sm flex flex-col gap-3 border-t border-gray-100 dark:border-zinc-700">
          <div className="flex flex-wrap gap-4 pt-3 text-gray-600 dark:text-zinc-300">
            <span>Fill: <strong className="text-gray-800 dark:text-zinc-100">{serializeFill(cfg.missing_values)}</strong></span>
            <span>Normalize: <strong className="text-gray-800 dark:text-zinc-100">{serializeNormalize(cfg.normalize)}</strong></span>
            <span>Nullable: <strong className="text-gray-800 dark:text-zinc-100">{cfg.nullable ? "yes" : "no"}</strong></span>
          </div>
          {hasSentinels && (
            <div>
              <span className="text-xs font-semibold uppercase text-gray-400 dark:text-zinc-500 tracking-wide">Sentinel values</span>
              <div className="flex flex-wrap gap-1 mt-1">
                {cfg.sentinel_values!.map((v, i) => (
                  <span key={i} className="px-2 py-0.5 bg-red-50 dark:bg-red-950 text-red-700 dark:text-red-400 rounded text-xs font-mono">{String(v)}</span>
                ))}
              </div>
            </div>
          )}
          {hasWarnings && (
            <div>
              <span className="text-xs font-semibold uppercase text-gray-400 dark:text-zinc-500 tracking-wide">Warnings</span>
              <ul className="mt-1 flex flex-col gap-0.5">
                {cfg.warnings.map((w, i) => <li key={i} className="text-amber-700 dark:text-amber-300 text-xs">⚠ {w}</li>)}
              </ul>
            </div>
          )}
          {cfg.note && (
            <div>
              <span className="text-xs font-semibold uppercase text-gray-400 dark:text-zinc-500 tracking-wide">Note</span>
              <p className="text-gray-500 dark:text-zinc-400 italic text-sm mt-0.5">{cfg.note}</p>
            </div>
          )}
          {cfg.transform_hint && (
            <div>
              <span className="text-xs font-semibold uppercase text-gray-400 dark:text-zinc-500 tracking-wide">Transform hint</span>
              <p className="text-blue-600 dark:text-blue-400 italic text-sm mt-0.5">{cfg.transform_hint}</p>
            </div>
          )}
          {cfg.transform_code && (
            <div>
              <span className="text-xs font-semibold uppercase text-gray-400 dark:text-zinc-500 tracking-wide">Transform code</span>
              <pre className="mt-1 bg-gray-900 text-green-400 text-xs rounded-lg px-4 py-3 overflow-x-auto font-mono">
                {cfg.transform_code}
              </pre>
            </div>
          )}
        </div>
      )}
    </div>
  )
}
