import type { DataPreview } from "../../types"

interface Props {
  data: DataPreview | null
  loading: boolean
  totalRows?: number
}

export function DataPreviewTable({ data, loading, totalRows }: Props) {
  if (loading) {
    return (
      <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-10 flex justify-center">
        <svg className="w-6 h-6 animate-spin text-blue-500" fill="none" viewBox="0 0 24 24">
          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8v4l3-3-3-3v4a8 8 0 00-8 8h4z" />
        </svg>
      </div>
    )
  }

  if (!data) {
    return (
      <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-8 text-center text-sm text-zinc-400 dark:text-zinc-500 italic">
        Preview not available
      </div>
    )
  }

  return (
    <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 overflow-hidden">
      <div className="overflow-auto max-h-[60vh]">
        <table className="min-w-full text-sm">
          <thead className="sticky top-0 z-10 bg-white dark:bg-zinc-900 border-b border-gray-200 dark:border-zinc-700">
            <tr>
              {data.columns.map((col) => (
                <th
                  key={col}
                  className="px-3 py-2 text-left text-xs font-semibold uppercase text-zinc-500 dark:text-zinc-400 whitespace-nowrap"
                >
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {data.rows.map((row, i) => (
              <tr
                key={i}
                className="border-b border-gray-100 dark:border-zinc-800 even:bg-gray-50 dark:even:bg-zinc-800/50"
              >
                {row.map((cell, j) => (
                  <td
                    key={j}
                    className={`px-3 py-2 whitespace-nowrap ${
                      cell === null
                        ? "text-zinc-400 dark:text-zinc-600 italic"
                        : "text-gray-700 dark:text-zinc-200"
                    }`}
                  >
                    {cell === null ? "—" : String(cell)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="px-3 py-2 text-xs text-zinc-400">
        {totalRows !== undefined && totalRows > data.rows.length
          ? `Showing first ${data.rows.length} of ${totalRows.toLocaleString()} rows`
          : `Showing first ${data.rows.length} rows`}
      </div>
    </div>
  )
}
