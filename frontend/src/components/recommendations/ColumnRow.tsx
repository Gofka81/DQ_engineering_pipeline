import { useRef, useState } from "react"
import type { ColumnConfig, MissingValuesFill } from "../../types"

function fillImpactLabel(mv: MissingValuesFill): string | null {
  const n = mv.null_count
  if (!n || n === 0) return null
  switch (mv.strategy) {
    case "drop_row":    return `${n} nulls → drop row`
    case "drop_column": return `→ column removed`
    case "fill":        return `${n} nulls → fill with ${mv.value ?? "?"}`
    case "mean":        return `${n} nulls → fill with mean`
    case "median":      return `${n} nulls → fill with median`
    case "mode":        return `${n} nulls → fill with mode`
    case "leave_null":  return `${n} nulls kept`
    default:            return null
  }
}

interface Props {
  name: string
  config: ColumnConfig
  onChange: (patch: Partial<ColumnConfig>) => void
}

const FILL_STRATEGIES = ["median", "mean", "mode", "fill", "drop_row", "drop_column", "leave_null"] as const
const TYPES = ["int", "float", "string", "date", "bool"] as const
const NORMALIZE_OPTIONS = [
  { value: "false", label: "None" },
  { value: "min_max", label: "Min-Max" },
  { value: "z_score", label: "Z-Score" },
] as const

const RENAME_RE = /^[a-zA-Z_][a-zA-Z0-9_]*$/

export function ColumnRow({ name, config, onChange }: Props) {
  const [renameErr, setRenameErr] = useState<string | null>(null)
  const [expanded, setExpanded] = useState(false)
  const renameInputRef = useRef<HTMLInputElement>(null)

  const isRenameActive = config.rename_to != null

  function handleRenameToggle(checked: boolean) {
    if (!checked) {
      onChange({ rename_to: null })
    } else {
      const val = renameInputRef.current?.value ?? ""
      if (val && RENAME_RE.test(val)) {
        setRenameErr(null)
        onChange({ rename_to: val })
      } else if (val) {
        setRenameErr("Must be a valid identifier (letters, digits, underscores; start with letter/_)")
      }
    }
  }

  const hasWarnings = config.warnings.length > 0
  const hasNote = !!config.note
  const hasHint = config.transform_hint != null && config.transform_hint !== ""
  const hasSentinels = !!(config.sentinel_values?.length)
  const hasContent = true // always expandable — transform hint can be added to any column

  function handleRenameBlur(v: string) {
    if (v && !RENAME_RE.test(v)) {
      setRenameErr("Must be a valid identifier (letters, digits, underscores; start with letter/_)")
    } else {
      setRenameErr(null)
      onChange({ rename_to: v || null })
    }
  }

  function handleNormalize(v: string) {
    onChange({ normalize: v === "false" ? false : (v as "min_max" | "z_score") })
  }

  const normalizeVal = config.normalize === false ? "false" : config.normalize
  const selectCls = "text-sm border border-gray-300 dark:border-zinc-600 rounded px-1 py-0.5 bg-white dark:bg-zinc-800 dark:text-zinc-100"
  const inputCls = "text-sm border border-gray-300 dark:border-zinc-600 rounded px-1 py-0.5 bg-white dark:bg-zinc-800 dark:text-zinc-100"

  return (
    <>
      <tr className={hasWarnings ? "bg-amber-50 dark:bg-amber-950/30" : "dark:bg-zinc-900"}>
        {/* Column name */}
        <td className="px-3 py-2 text-sm font-mono text-gray-800 dark:text-zinc-200 whitespace-nowrap">
          {name}
          {config.rename_to && (
            <span className="ml-1 text-xs text-blue-500">→ {config.rename_to}</span>
          )}
        </td>

        {/* Type */}
        <td className="px-3 py-2">
          <select
            value={config.type}
            onChange={(e) => {
              const newType = e.target.value as ColumnConfig["type"]
              const patch: Partial<ColumnConfig> = { type: newType }
              if (newType !== "int" && newType !== "float") patch.normalize = false
              onChange(patch)
            }}
            className={selectCls}
          >
            {TYPES.map((t) => <option key={t} value={t}>{t}</option>)}
          </select>
        </td>

        {/* Fill strategy */}
        <td className="px-3 py-2">
          <select
            value={config.missing_values?.strategy ?? ""}
            onChange={(e) => {
              const s = e.target.value as (typeof FILL_STRATEGIES)[number]
              const nullCount = config.missing_values?.null_count
              onChange({ missing_values: s ? { strategy: s, value: null, null_count: nullCount } : null })
            }}
            className={selectCls}
          >
            <option value="">—</option>
            {FILL_STRATEGIES.map((s) => <option key={s} value={s}>{s}</option>)}
          </select>
          {config.missing_values?.strategy === "fill" && (
            <input
              type="text"
              placeholder="value"
              defaultValue={config.missing_values.value ?? ""}
              onBlur={(e) => onChange({ missing_values: { strategy: "fill", value: e.target.value, null_count: config.missing_values?.null_count } })}
              className={`ml-1 w-20 ${inputCls}`}
            />
          )}
          {config.missing_values && (() => {
            const label = fillImpactLabel(config.missing_values!)
            return label ? (
              <span className="ml-1.5 text-xs text-zinc-400 dark:text-zinc-500">{label}</span>
            ) : null
          })()}
        </td>

        {/* Normalize — only numeric types can be normalised */}
        <td className="px-3 py-2">
          {(config.type === "int" || config.type === "float") ? (
            <select
              value={normalizeVal}
              onChange={(e) => handleNormalize(e.target.value)}
              className={selectCls}
            >
              {NORMALIZE_OPTIONS.map((o) => <option key={o.value} value={o.value}>{o.label}</option>)}
            </select>
          ) : (
            <span className="text-xs text-gray-400 dark:text-zinc-600">—</span>
          )}
        </td>

        {/* Rename */}
        <td className="px-3 py-2">
          <div className="flex items-center gap-1.5">
            <input
              ref={renameInputRef}
              type="text"
              defaultValue={config.rename_to ?? ""}
              placeholder="rename_to"
              onBlur={(e) => isRenameActive && handleRenameBlur(e.target.value)}
              className={`w-28 ${inputCls} ${!isRenameActive ? "opacity-40" : ""}`}
            />
            <input
              type="checkbox"
              checked={isRenameActive}
              onChange={(e) => handleRenameToggle(e.target.checked)}
              title={isRenameActive ? "Disable rename" : "Enable rename"}
              className="accent-blue-500 cursor-pointer"
            />
          </div>
          {renameErr && <p className="text-red-500 text-xs mt-0.5">{renameErr}</p>}
        </td>

        {/* Info icons + expand toggle */}
        <td className="px-3 py-2">
          <div className="flex items-center gap-1.5">
            {hasWarnings && (
              <span className="flex items-center gap-0.5 text-amber-500" title={`${config.warnings.length} warning(s)`}>
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M12 9v3.75m-9.303 3.376c-.866 1.5.217 3.374 1.948 3.374h14.71c1.73 0 2.813-1.874 1.948-3.374L13.949 3.378c-.866-1.5-3.032-1.5-3.898 0L2.697 16.126zM12 15.75h.007v.008H12v-.008z" />
                </svg>
                {config.warnings.length > 1 && (
                  <span className="text-xs font-medium">{config.warnings.length}</span>
                )}
              </span>
            )}
            {hasNote && (
              <span className="text-gray-400" title="LLM note">
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M7.5 8.25h9m-9 3H12m-9.75 1.51c0 1.6 1.123 2.994 2.707 3.227 1.129.166 2.27.293 3.423.379.35.026.67.21.865.501L12 21l2.755-4.133a1.14 1.14 0 01.865-.501 48.172 48.172 0 003.423-.379c1.584-.233 2.707-1.626 2.707-3.228V6.741c0-1.602-1.123-2.995-2.707-3.228A48.394 48.394 0 0012 3c-2.392 0-4.744.175-7.043.513C3.373 3.746 2.25 5.14 2.25 6.741v6.018z" />
                </svg>
              </span>
            )}
            {hasHint && (
              <span className="text-blue-400" title="Transform hint">
                <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M9.813 15.904L9 18.75l-.813-2.846a4.5 4.5 0 00-3.09-3.09L2.25 12l2.846-.813a4.5 4.5 0 003.09-3.09L9 5.25l.813 2.846a4.5 4.5 0 003.09 3.09L15.75 12l-2.846.813a4.5 4.5 0 00-3.09 3.09z" />
                </svg>
              </span>
            )}
            {hasContent && (
              <button
                onClick={() => setExpanded((e) => !e)}
                className="text-gray-400 hover:text-gray-600 dark:hover:text-zinc-300 transition-colors ml-0.5"
                title={expanded ? "Collapse" : "Expand"}
              >
                <svg
                  className={`w-3.5 h-3.5 transition-transform ${expanded ? "rotate-180" : ""}`}
                  fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}
                >
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19.5 8.25l-7.5 7.5-7.5-7.5" />
                </svg>
              </button>
            )}
          </div>
        </td>
      </tr>

      {/* Expanded detail row */}
      {expanded && (
        <tr className="bg-gray-50 dark:bg-zinc-800/50">
          <td colSpan={6} className="px-4 py-3 text-xs">
            <div className="flex flex-col gap-3">
              {hasWarnings && (
                <div>
                  <p className="font-semibold text-amber-600 dark:text-amber-400 mb-1.5">Warnings</p>
                  <ul className="flex flex-col gap-1">
                    {config.warnings.map((w, i) => (
                      <li key={i} className="text-amber-700 dark:text-amber-300">• {w}</li>
                    ))}
                  </ul>
                </div>
              )}
              {hasNote && (
                <div>
                  <p className="font-semibold text-gray-500 dark:text-zinc-400 mb-1">Note</p>
                  <p className="text-gray-500 dark:text-zinc-400 italic">{config.note}</p>
                </div>
              )}
              {hasSentinels && (
                <div>
                  <div className="flex items-center gap-2 mb-1">
                    <p className="font-semibold text-red-500">Sentinel values</p>
                    <label className="flex items-center gap-1 text-gray-500 dark:text-zinc-400 cursor-pointer select-none">
                      <input
                        type="checkbox"
                        checked={config.replace_sentinels !== false}
                        onChange={(e) => onChange({ replace_sentinels: e.target.checked })}
                        className="accent-red-500 cursor-pointer"
                      />
                      replace with null
                    </label>
                  </div>
                  <div className="flex flex-wrap gap-1">
                    {config.sentinel_values!.map((v, i) => (
                      <span key={i} className="px-2 py-0.5 bg-red-50 dark:bg-red-950 text-red-700 dark:text-red-400 rounded font-mono">{String(v)}</span>
                    ))}
                  </div>
                </div>
              )}
              <div>
                <p className="font-semibold text-blue-500 mb-1.5">Transform hint</p>
                <div className="flex items-start gap-2">
                  <textarea
                    defaultValue={config.transform_hint ?? ""}
                    placeholder="Describe what transformation is needed…"
                    onBlur={(e) => onChange({ transform_hint: e.target.value || null })}
                    rows={2}
                    className="flex-1 border border-gray-300 dark:border-zinc-600 rounded px-2 py-1 bg-white dark:bg-zinc-800 dark:text-zinc-100 resize-none"
                  />
                  {hasHint && (
                    <button
                      onClick={() => onChange({ transform_hint: null })}
                      className="text-gray-400 hover:text-red-500 transition-colors mt-0.5"
                      title="Discard transform hint"
                    >
                      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                        <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                      </svg>
                    </button>
                  )}
                </div>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  )
}
