import { useCallback, useEffect, useRef, useState } from "react";

export interface ResourceState<T> {
  data: T | null;
  error: Error | null;
  loading: boolean;
  refreshing: boolean;
  lastUpdated: Date | null;
  refresh: () => void;
}

export function usePollingResource<T>(
  loader: (signal: AbortSignal) => Promise<T>,
  intervalMs: number | null,
): ResourceState<T> {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<Error | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const active = useRef<AbortController | null>(null);
  const hasData = useRef(false);

  const load = useCallback(async () => {
    if (active.current) return;
    const controller = new AbortController();
    active.current = controller;
    if (hasData.current) setRefreshing(true);
    try {
      const result = await loader(controller.signal);
      setData(result);
      hasData.current = true;
      setError(null);
      setLastUpdated(new Date());
    } catch (caught) {
      if (!controller.signal.aborted) {
        setError(
          caught instanceof Error ? caught : new Error("Request failed"),
        );
      }
    } finally {
      if (active.current === controller) active.current = null;
      if (!controller.signal.aborted) {
        setLoading(false);
        setRefreshing(false);
      }
    }
  }, [loader]);

  useEffect(() => {
    void load();
    if (intervalMs === null) return () => active.current?.abort();
    const timer = window.setInterval(() => {
      if (document.visibilityState === "visible") void load();
    }, intervalMs);
    return () => {
      window.clearInterval(timer);
      active.current?.abort();
    };
  }, [intervalMs, load]);

  return {
    data,
    error,
    loading,
    refreshing,
    lastUpdated,
    refresh: () => void load(),
  };
}
