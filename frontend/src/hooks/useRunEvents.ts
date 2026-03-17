import { useEffect, useRef } from "react"
import { API_URL } from "../api/client"
import type { RunStatusEvent } from "../types"

const TERMINAL = new Set(["COMPLETED", "FAILED"])

export function useRunEvents(
  fileId: string | null,
  onUpdate: (data: RunStatusEvent) => void
): void {
  const doneRef = useRef(false)

  useEffect(() => {
    if (!fileId) return
    doneRef.current = false

    const token = localStorage.getItem("token")
    if (!token) return

    const url = `${API_URL}/api/files/${fileId}/events?token=${encodeURIComponent(token)}`
    const source = new EventSource(url)

    source.onmessage = (e) => {
      try {
        const data = JSON.parse(e.data) as RunStatusEvent
        onUpdate(data)
        if (TERMINAL.has(data.status)) {
          doneRef.current = true
          source.close()
        }
      } catch {
        // ignore malformed
      }
    }

    source.onerror = () => {
      // Only reconnect if not yet terminal
      if (doneRef.current) source.close()
    }

    return () => source.close()
  }, [fileId])
}
