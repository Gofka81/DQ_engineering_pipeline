export function formatDate(iso: string): string {
  return new Date(iso).toLocaleDateString("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
  })
}

export function formatScore(score: number): string {
  return score.toFixed(1)
}

export function scoreDelta(before: number, after: number): string {
  const d = after - before
  return (d >= 0 ? "+" : "") + d.toFixed(1)
}
