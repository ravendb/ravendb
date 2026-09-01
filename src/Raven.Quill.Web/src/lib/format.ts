const compactNumberFormatter = new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
});

// Headline-friendly number, e.g. 1100 -> "1.1K", 18_400_000 -> "18.4M".
export function formatCompact(value: number): string {
    return compactNumberFormatter.format(value);
}

// Prefer <Timestamp> over calling these directly.
const dateFormatter = new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", year: "numeric" });
const dateTimeFormatter = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
});

// e.g. "Jul 14, 2026". Falls back to the raw value if it doesn't parse.
export function formatDate(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : dateFormatter.format(date);
}

// e.g. "Jul 14, 2026, 3:28 PM". Falls back to the raw value if it doesn't parse.
export function formatDateTime(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : dateTimeFormatter.format(date);
}

const MINUTE_MS = 60_000;
const HOUR_MS = 60 * MINUTE_MS;
const DAY_MS = 24 * HOUR_MS;

// Largest unit first: a duration is shown in the biggest unit it fills at least once.
const RELATIVE_TIME_UNITS: { unit: Intl.RelativeTimeFormatUnit; ms: number }[] = [
    { unit: "year", ms: 365 * DAY_MS },
    { unit: "month", ms: 30 * DAY_MS },
    { unit: "week", ms: 7 * DAY_MS },
    { unit: "day", ms: DAY_MS },
    { unit: "hour", ms: HOUR_MS },
    { unit: "minute", ms: MINUTE_MS },
];

const relativeTimeFormatter = new Intl.RelativeTimeFormat("en-US", { numeric: "always", style: "long" });

// e.g. "17 hours ago", "1 day ago", "in 3 days".
export function formatRelativeTime(value: string): string {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return value;
    }

    const elapsedMs = date.getTime() - Date.now();
    for (const { unit, ms } of RELATIVE_TIME_UNITS) {
        if (Math.abs(elapsedMs) >= ms) {
            return relativeTimeFormatter.format(Math.round(elapsedMs / ms), unit);
        }
    }
    return "just now";
}

const DURATION_UNITS = [
    { label: "day", seconds: 86_400 },
    { label: "hour", seconds: 3_600 },
    { label: "minute", seconds: 60 },
    { label: "second", seconds: 1 },
] as const;

// Duration rounded down to its largest whole unit, e.g. 5 -> "5 seconds", 7_200 -> "2 hours".
export function formatDuration(totalSeconds: number): string {
    const unit =
        DURATION_UNITS.find(({ seconds }) => totalSeconds >= seconds) ?? DURATION_UNITS[DURATION_UNITS.length - 1];
    const value = Math.floor(totalSeconds / unit.seconds);
    return `${value} ${unit.label}${value === 1 ? "" : "s"}`;
}

const currencyFormatter = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

// Currency display, e.g. 128.4 -> "$128.40".
export function formatCurrency(value: number): string {
    return currencyFormatter.format(value);
}
