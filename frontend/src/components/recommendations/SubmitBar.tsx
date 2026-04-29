import { useState } from "react"

interface Props {
  onSubmit: () => void
  disabled?: boolean
  loading?: boolean
}

export function SubmitBar({ onSubmit, disabled, loading }: Props) {
  const [confirming, setConfirming] = useState(false)

  if (confirming) {
    return (
      <div className="flex-shrink-0 bg-white dark:bg-zinc-900 border-t border-gray-200 dark:border-zinc-700 px-4 py-3.5 flex items-center justify-end gap-3">
        <span className="text-sm text-gray-600 dark:text-zinc-300">Apply these transformations?</span>
        <button
          onClick={() => setConfirming(false)}
          className="px-4 py-2 text-sm border border-gray-300 dark:border-zinc-600 rounded-lg hover:bg-gray-50 dark:hover:bg-zinc-800 dark:text-zinc-300"
        >
          Cancel
        </button>
        <button
          onClick={() => { setConfirming(false); onSubmit() }}
          disabled={disabled || loading}
          className="px-4 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
        >
          {loading ? "Submitting..." : "Confirm"}
        </button>
      </div>
    )
  }

  return (
    <div className="flex-shrink-0 bg-white dark:bg-zinc-900 border-t border-gray-200 dark:border-zinc-700 px-4 py-3.5 flex justify-end">
      <button
        onClick={() => setConfirming(true)}
        disabled={disabled || loading}
        className="px-5 py-2 text-sm bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 font-medium"
      >
        Apply Transformations
      </button>
    </div>
  )
}
