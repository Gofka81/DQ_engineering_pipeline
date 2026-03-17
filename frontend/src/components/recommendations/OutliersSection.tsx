import type { OutliersConfig } from "../../types"

interface Props {
  outliers: Record<string, OutliersConfig>
  onChange: (col: string, strategy: OutliersConfig["strategy"]) => void
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
          <tbody className="divide-y divide-gray-100 dark:divide-zinc-700">
            {entries.map(([col, cfg]) => (
              <tr key={col}>
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
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
