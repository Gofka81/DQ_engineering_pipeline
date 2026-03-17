import type { DQScores } from "../../types"
import { DQScoreBar } from "./DQScoreBar"

interface Props {
  before: DQScores
  after?: DQScores
}

export function ScoreComparison({ before, after }: Props) {
  return (
    <div className="flex flex-col gap-2">
      <DQScoreBar label="Overall" before={before.overall} after={after?.overall} prominent />
      <DQScoreBar label="Completeness" before={before.completeness} after={after?.completeness} />
      <DQScoreBar label="Uniqueness" before={before.uniqueness} after={after?.uniqueness} />
      <DQScoreBar label="Validity" before={before.validity} after={after?.validity} />
      <DQScoreBar label="Consistency" before={before.consistency} after={after?.consistency} />
    </div>
  )
}
