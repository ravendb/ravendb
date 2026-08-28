import { clsx, type ClassValue } from "clsx";
import { toast } from "sonner";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs));
}

export function tryParseJson<T>(value: string): T | null {
    try {
        return JSON.parse(value) as T;
    } catch {
        return null;
    }
}

// Renders an ISO timestamp in the viewer's locale; falls back to the raw value
// if it isn't a parseable date.
export function formatDateTime(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

const SECOND_MS = 1000;
const MINUTE_MS = 60 * SECOND_MS;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;
const WEEK_MS = 7 * DAY_MS;
const MONTH_MS = 30 * DAY_MS;
const YEAR_MS = 365 * DAY_MS;

// Largest unit first: a duration is shown in the biggest unit it fills at least once.
const RELATIVE_TIME_UNITS: { unit: Intl.RelativeTimeFormatUnit; ms: number }[] = [
    { unit: "year", ms: YEAR_MS },
    { unit: "month", ms: MONTH_MS },
    { unit: "week", ms: WEEK_MS },
    { unit: "day", ms: DAY_MS },
    { unit: "hour", ms: HOUR_MS },
    { unit: "minute", ms: MINUTE_MS },
    { unit: "second", ms: SECOND_MS },
];

// "always" (not "auto") so a value just past the day boundary reads "in 1 day" rather than
// "tomorrow" — the vaguer word can clash with a caller's own warning threshold (see getExpiryStatus).
const relativeTimeFormatter = new Intl.RelativeTimeFormat("en", { numeric: "always", style: "narrow" });

// Renders an ISO timestamp or epoch milliseconds as a short relative label (e.g. "19m ago",
// "1h ago"); falls back to the raw value if it isn't a parseable date.
export function formatRelativeTime(value: string | number) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return String(value);
    }

    const elapsedMs = date.getTime() - Date.now();
    for (const { unit, ms } of RELATIVE_TIME_UNITS) {
        if (Math.abs(elapsedMs) >= ms) {
            return relativeTimeFormatter.format(Math.round(elapsedMs / ms), unit);
        }
    }
    return "now"; // sub-second differences
}

export async function copyToClipboard(value: string) {
    try {
        await navigator.clipboard.writeText(value);
        toast.success("Copied to clipboard");
    } catch {
        toast.error("Could not copy to clipboard");
    }
}
