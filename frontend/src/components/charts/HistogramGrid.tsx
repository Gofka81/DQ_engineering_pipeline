import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer } from "recharts"
import { useTheme } from "../../hooks/useTheme"
import type { EDAColumn, EDAColumnNumeric, EDAHistogram } from "../../types"

function formatNum(n: number): string {
  const abs = Math.abs(n)
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return n % 1 === 0 ? String(Math.round(n)) : n.toFixed(1)
}

function HistogramChart({
  colName,
  histogram,
  isDark,
}: {
  colName: string
  histogram: EDAHistogram
  isDark: boolean
}) {
  const data = histogram.counts.map((count, i) => ({
    bin: formatNum(histogram.edges[i]),
    count,
  }))

  return (
    <div className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-4">
      <p className="text-xs font-medium text-gray-600 dark:text-zinc-400 mb-2 truncate">{colName}</p>
      <ResponsiveContainer width="100%" height={110}>
        <BarChart data={data} margin={{ top: 2, right: 4, left: -24, bottom: 0 }}>
          <XAxis
            dataKey="bin"
            tick={{ fontSize: 9, fill: isDark ? "#71717a" : "#9ca3af" }}
            interval={4}
            axisLine={false}
            tickLine={false}
          />
          <YAxis
            tick={{ fontSize: 9, fill: isDark ? "#71717a" : "#9ca3af" }}
            axisLine={false}
            tickLine={false}
          />
          <Tooltip
            contentStyle={{
              fontSize: 11,
              background: isDark ? "#27272a" : "#fff",
              border: `1px solid ${isDark ? "#3f3f46" : "#e5e7eb"}`,
              color: isDark ? "#e4e4e7" : "#374151",
              borderRadius: 6,
              padding: "4px 8px",
            }}
            formatter={(v: number) => [v.toLocaleString(), "count"]}
          />
          <Bar dataKey="count" fill="#3b82f6" radius={[2, 2, 0, 0]} />
        </BarChart>
      </ResponsiveContainer>
    </div>
  )
}

export function HistogramGrid({ columns }: { columns: Record<string, EDAColumn> }) {
  const { theme } = useTheme()
  const isDark = theme === "dark"

  const numericCols = Object.entries(columns).filter(
    ([, col]) =>
      col.detected_type === "numeric" &&
      (col as EDAColumnNumeric).stats?.histogram != null
  ) as [string, EDAColumnNumeric][]

  if (numericCols.length === 0) return null

  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-3">
        Distributions — Numeric Columns
      </p>
      <div className="grid grid-cols-2 lg:grid-cols-3 gap-3">
        {numericCols.map(([colName, col]) => (
          <HistogramChart
            key={colName}
            colName={colName}
            histogram={col.stats.histogram!}
            isDark={isDark}
          />
        ))}
      </div>
    </div>
  )
}
