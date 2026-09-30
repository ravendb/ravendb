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

const currencyFormatter = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

// Currency display, e.g. 128.4 -> "$128.40".
export function formatCurrency(value: number): string {
    return currencyFormatter.format(value);
}
