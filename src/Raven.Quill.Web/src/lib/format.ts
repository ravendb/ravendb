const compactNumberFormatter = new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
});

// Headline-friendly number, e.g. 1100 -> "1.1K", 18_400_000 -> "18.4M".
export function formatCompact(value: number): string {
    return compactNumberFormatter.format(value);
}

const fullDateFormatter = new Intl.DateTimeFormat("en-US", { month: "long", day: "numeric", year: "numeric" });

// Full date, e.g. "July 14, 2026". Falls back to the raw value if it doesn't parse.
export function formatDate(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : fullDateFormatter.format(date);
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
