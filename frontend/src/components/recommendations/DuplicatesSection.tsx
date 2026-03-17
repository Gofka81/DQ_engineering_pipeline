import type { DuplicatesConfig } from "../../types"

interface Props {
  config: DuplicatesConfig | null
  onChange: (patch: Partial<DuplicatesConfig>) => void
}

export function DuplicatesSection({ config, onChange }: Props) {
  if (!config?.strategy) return null
  const selectCls = "text-sm border border-gray-300 dark:border-zinc-600 rounded px-2 py-1 bg-white dark:bg-zinc-800 dark:text-zinc-100"
  return (
    <div className="border border-gray-200 dark:border-zinc-700 rounded-lg p-4 bg-white dark:bg-zinc-900">
      <h3 className="text-sm font-semibold text-gray-700 dark:text-zinc-300 mb-3">Duplicates</h3>
      <div className="flex gap-4 flex-wrap">
        <div>
          <label className="text-xs text-gray-500 dark:text-zinc-400 block mb-1">Strategy</label>
          <select
            value={config.strategy}
            onChange={(e) => onChange({ strategy: e.target.value as DuplicatesConfig["strategy"] })}
            className={selectCls}
          >
            <option value="ignore">ignore</option>
            <option value="drop">drop</option>
          </select>
        </div>
        {config.strategy === "drop" && (
          <div>
            <label className="text-xs text-gray-500 dark:text-zinc-400 block mb-1">Keep</label>
            <select
              value={config.keep}
              onChange={(e) => onChange({ keep: e.target.value as DuplicatesConfig["keep"] })}
              className={selectCls}
            >
              <option value="first">first</option>
              <option value="last">last</option>
            </select>
          </div>
        )}
      </div>
    </div>
  )
}
