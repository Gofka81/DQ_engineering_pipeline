import { Radar, RadarChart, PolarGrid, PolarAngleAxis, ResponsiveContainer } from "recharts"
import { useTheme } from "../../hooks/useTheme"
import type { DQScores } from "../../types"

const AXES = [
  { key: "completeness", label: "Completeness" },
  { key: "uniqueness", label: "Uniqueness" },
  { key: "validity", label: "Validity" },
  { key: "consistency", label: "Consistency" },
]

export function DQRadarChart({ scores }: { scores: DQScores }) {
  const { theme } = useTheme()
  const isDark = theme === "dark"

  const data = AXES.map(({ key, label }) => ({
    axis: label,
    value: Math.round((scores[key as keyof DQScores] as number) ?? 0),
  }))

  return (
    <ResponsiveContainer width="100%" height={240}>
      <RadarChart data={data} cx="50%" cy="50%" outerRadius="70%">
        <PolarGrid stroke={isDark ? "#3f3f46" : "#e5e7eb"} gridType="polygon" />
        <PolarAngleAxis
          dataKey="axis"
          tick={{ fontSize: 11, fill: isDark ? "#a1a1aa" : "#6b7280" }}
        />
        <Radar
          dataKey="value"
          stroke="#3b82f6"
          fill="#3b82f6"
          fillOpacity={0.25}
          dot={false}
        />
      </RadarChart>
    </ResponsiveContainer>
  )
}
