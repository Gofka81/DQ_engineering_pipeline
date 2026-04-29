import type { OutliersConfig } from "../../types"

interface Props {
  outliers: Record<string, OutliersConfig>
  onChange: (col: string, strategy: OutliersConfig["strategy"]) => void
}

function outlierImpactLabel(o: OutliersConfig): string {
  const n = o.count ?? 0
  switch (o.strategy) {
    case "winsorise": return `${n} → winsorize [${o.lower}–${o.upper}]`
    case "cap":       return `${n} → cap to [${o.lower}–${o.upper}]`
    case "remove":    return `${n} → remove rows`
    case "keep":      return `${n} outliers kept`
    default:          return `${n} outliers`
  }
}

export function OutliersSection({ outliers, onChange }: Props) {
  const entries = Object.entries(outliers)
  if (entries.length === 0) return null

  return (
    <div className="border border-gray-200 dark:border-zinc-700 rounded-lg p-4 bg-white dark:bg-zinc-900">
      <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-3">Outliers</h3>
      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-xs text-gray-500 dark:text-zinc-400 border-b border-gray-200 dark:border-zinc-700">
              <th className="pb-2 text-left">Column</th>
              <th className="pb-2 text-left">Count</th>
              <th className="pb-2 text-left">IQR Bounds</th>
              <th className="pb-2 text-left">Strategy</th>
            </tr>
          </thead>
          <tbody>
            {entries.map(([col, cfg]) => (
              <>
                <tr key={col} className="border-t border-gray-100 dark:border-zinc-700">
                  <td className="py-2 font-mono text-gray-800 dark:text-zinc-200">{col}</td>
                  <td className="py-2 text-gray-600 dark:text-zinc-400">{cfg.count ?? "—"}</td>
                  <td className="py-2 text-gray-600 dark:text-zinc-400">
                    {cfg.lower !== null && cfg.upper !== null
                      ? `${cfg.lower?.toLocaleString()} – ${cfg.upper?.toLocaleString()}`
                      : "—"}
                  </td>
                  <td className="py-2">
                    <select
                      value={cfg.strategy}
                      onChange={(e) => onChange(col, e.target.value as OutliersConfig["strategy"])}
                      className="text-sm border border-gray-300 dark:border-zinc-600 rounded px-1 py-0.5 bg-white dark:bg-zinc-800 dark:text-zinc-100"
                    >
                      {["keep", "winsorise", "remove", "cap"].map((s) => (
                        <option key={s} value={s}>{s}</option>
                      ))}
                    </select>
                    <span className="ml-1.5 text-xs text-zinc-400 dark:text-zinc-500">
                      {outlierImpactLabel(cfg)}
                    </span>
                  </td>
                </tr>
                {cfg.note && (
                  <tr key={`${col}-note`}>
                    <td colSpan={4} className="pb-2 pt-0">
                      <p className="text-xs text-gray-500 dark:text-zinc-400 italic pl-0.5">{cfg.note}</p>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
