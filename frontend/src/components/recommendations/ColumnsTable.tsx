import type { Recommendations, ColumnConfig } from "../../types"
import { ColumnRow } from "./ColumnRow"

interface Props {
  columns: Recommendations["columns"]
  onChange: (name: string, patch: Partial<ColumnConfig>) => void
  onClearAllRenames?: () => void
}

export function ColumnsTable({ columns, onChange, onClearAllRenames }: Props) {
  const hasRenames = Object.values(columns).some((c) => c.rename_to)

  return (
    <div className="overflow-x-auto rounded-lg border border-gray-200 dark:border-zinc-700">
      {hasRenames && onClearAllRenames && (
        <div className="px-3 py-1.5 border-b border-gray-200 dark:border-zinc-700 flex justify-end bg-gray-50 dark:bg-zinc-800">
          <button
            onClick={onClearAllRenames}
            className="text-xs text-gray-500 dark:text-zinc-400 hover:text-red-500 dark:hover:text-red-400 transition-colors"
          >
            Clear all renames
          </button>
        </div>
      )}
      <table className="w-full text-left">
        <thead className="bg-gray-50 dark:bg-zinc-800 border-b border-gray-200 dark:border-zinc-700">
          <tr>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Column</th>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Type</th>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Fill Strategy</th>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Normalize</th>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Rename To</th>
            <th className="px-3 py-2 text-xs font-semibold text-gray-500 dark:text-zinc-400">Info</th>
          </tr>
        </thead>
        <tbody className="divide-y divide-gray-100 dark:divide-zinc-700">
          {Object.entries(columns).map(([name, cfg]) => (
            <ColumnRow
              key={name}
              name={name}
              config={cfg}
              onChange={(patch) => onChange(name, patch)}
            />
          ))}
        </tbody>
      </table>
    </div>
  )
}
