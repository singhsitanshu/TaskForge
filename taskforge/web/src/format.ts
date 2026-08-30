export function shortId(id: string): string {
  return id.length > 12 ? `${id.slice(0, 8)}…${id.slice(-4)}` : id;
}

export function formatTimestamp(value: string | null): string {
  if (!value) return "Not reported";
  const date = new Date(value);
  return new Intl.DateTimeFormat(undefined, {
    dateStyle: "medium",
    timeStyle: "medium",
  }).format(date);
}

export function formatRelative(value: string | null): string {
  if (!value) return "Not reported";
  const seconds = Math.round((new Date(value).getTime() - Date.now()) / 1000);
  const absolute = Math.abs(seconds);
  const formatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto" });
  if (absolute < 60) return formatter.format(seconds, "second");
  if (absolute < 3600)
    return formatter.format(Math.round(seconds / 60), "minute");
  if (absolute < 86400)
    return formatter.format(Math.round(seconds / 3600), "hour");
  return formatter.format(Math.round(seconds / 86400), "day");
}

export function formatDuration(
  start: string | null,
  end: string | null,
): string {
  if (!start || !end) return "Not available";
  const milliseconds = new Date(end).getTime() - new Date(start).getTime();
  if (milliseconds < 0) return "Invalid timing";
  if (milliseconds < 1) return `${Math.round(milliseconds * 1000)} µs`;
  if (milliseconds < 1000)
    return `${milliseconds.toFixed(milliseconds < 10 ? 2 : 1)} ms`;
  if (milliseconds < 60_000) return `${(milliseconds / 1000).toFixed(2)} s`;
  return `${Math.floor(milliseconds / 60_000)}m ${Math.round((milliseconds % 60_000) / 1000)}s`;
}
