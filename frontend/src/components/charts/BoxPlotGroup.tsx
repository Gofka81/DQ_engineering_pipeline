import { useRef, useEffect, useState } from "react"
import type { EDAColumn, EDAColumnNumeric, EDAProfile, OutliersConfig } from "../../types"

function isNumericWithBox(col: EDAColumn): col is EDAColumnNumeric {
  return (
    col.detected_type === "numeric" &&
    (col as EDAColumnNumeric).stats?.q1 != null &&
    (col as EDAColumnNumeric).stats?.q3 != null
  )
}

function formatNum(n: number): string {
  const abs = Math.abs(n)
  if (abs >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (abs >= 1_000) return `${(n / 1_000).toFixed(1)}k`
  return n % 1 === 0 ? String(Math.round(n)) : n.toFixed(1)
}

const LABEL_COL_W = 112 // w-28
const GAP = 8            // gap-2
const PAD = 4
const BOX_AREA_H = 28
const LABEL_H = 14
const ROW_H = BOX_AREA_H + LABEL_H
const MID_Y = BOX_AREA_H / 2
const BOX_H = 14

interface Props {
  edaColumns: EDAProfile["columns"]
  outliers: Record<string, OutliersConfig>
}

export function BoxPlotGroup({ edaColumns, outliers }: Props) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [svgW, setSvgW] = useState(380)

  useEffect(() => {
    const el = containerRef.current
    if (!el) return
    const obs = new ResizeObserver(entries => {
      setSvgW(Math.max(200, entries[0].contentRect.width - LABEL_COL_W - GAP))
    })
    obs.observe(el)
    return () => obs.disconnect()
  }, [])

  const entries = Object.entries(edaColumns).filter(([, col]) =>
    isNumericWithBox(col)
  ) as [string, EDAColumnNumeric][]

  if (entries.length === 0) return null

  const innerW = svgW - PAD * 2

  return (
    <div>
      <p className="text-xs font-semibold uppercase tracking-wide text-gray-400 dark:text-zinc-500 mb-3">
        Box Plots
      </p>
      <div
        ref={containerRef}
        className="bg-white dark:bg-zinc-900 rounded-xl border border-gray-200 dark:border-zinc-700 p-4 max-w-5xl"
      >
        {entries.map(([colName, col]) => {
          const { min, max, q1, q3, median } = col.stats
          const colMin = min as number
          const colMax = max as number
          const range = colMax - colMin || 1
          const scale = (v: number) => PAD + ((v - colMin) / range) * innerW

          const x1   = scale(colMin)
          const xQ1  = scale(q1!)
          const xMed = scale(median as number)
          const xQ3  = scale(q3!)
          const x2   = scale(colMax)
          const info = outliers[colName]

          return (
            <div key={colName} className="flex items-center gap-2">
              <div className="w-28 text-xs text-right text-gray-500 dark:text-zinc-400 truncate flex-shrink-0 pr-2">
                {colName}
              </div>
              <svg width={svgW} height={ROW_H} className="flex-shrink-0">
                <line x1={x1}  y1={MID_Y} x2={xQ1} y2={MID_Y} stroke="#94a3b8" strokeWidth={1.5} />
                <line x1={xQ3} y1={MID_Y} x2={x2}  y2={MID_Y} stroke="#94a3b8" strokeWidth={1.5} />
                <rect
                  x={xQ1} y={MID_Y - BOX_H / 2}
                  width={Math.max(xQ3 - xQ1, 2)} height={BOX_H}
                  fill="#3b82f6" fillOpacity={0.2}
                  stroke="#3b82f6" strokeWidth={1.5} rx={2}
                />
                <line
                  x1={xMed} y1={MID_Y - BOX_H / 2}
                  x2={xMed} y2={MID_Y + BOX_H / 2}
                  stroke="#3b82f6" strokeWidth={2}
                />
                <line x1={x1} y1={MID_Y - 5} x2={x1} y2={MID_Y + 5} stroke="#94a3b8" strokeWidth={1.5} />
                <line x1={x2} y1={MID_Y - 5} x2={x2} y2={MID_Y + 5} stroke="#94a3b8" strokeWidth={1.5} />
                {info && (info.count ?? 0) > 0 && (
                  <>
                    {info.lower != null && info.lower > colMin && (
                      <circle cx={scale(info.lower)} cy={MID_Y} r={3} fill="#f59e0b" />
                    )}
                    {info.upper != null && info.upper < colMax && (
                      <circle cx={scale(info.upper)} cy={MID_Y} r={3} fill="#f59e0b" />
                    )}
                  </>
                )}
                <text x={PAD}        y={BOX_AREA_H + 11} textAnchor="start" fontSize={9} fill="#71717a">{formatNum(colMin)}</text>
                <text x={svgW - PAD} y={BOX_AREA_H + 11} textAnchor="end"   fontSize={9} fill="#71717a">{formatNum(colMax)}</text>
              </svg>
            </div>
          )
        })}
      </div>
    </div>
  )
}
