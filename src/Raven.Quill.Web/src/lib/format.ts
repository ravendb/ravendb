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
const utcDateFormatter = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    timeZone: "UTC",
});
const utcDateTimeFormatter = new Intl.DateTimeFormat("en-US", {
    month: "short",
    day: "numeric",
    year: "numeric",
    hour: "numeric",
    minute: "2-digit",
    timeZone: "UTC",
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

// e.g. "Jul 14, 2026 UTC". For dates inside a UTC-filtered period.
export function formatDateUtc(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : `${utcDateFormatter.format(date)} UTC`;
}

// e.g. "Jul 14, 2026, 3:28 PM UTC". For dates inside a UTC-filtered period.
export function formatDateTimeUtc(value: string): string {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : `${utcDateTimeFormatter.format(date)} UTC`;
}

const currencyFormatter = new Intl.NumberFormat("en-US", { style: "currency", currency: "USD" });

// Currency display, e.g. 128.4 -> "$128.40".
export function formatCurrency(value: number): string {
    return currencyFormatter.format(value);
}

// e.g. (1, "table") -> "1 table", (3, "table") -> "3 tables".
export function formatCount(count: number, singular: string, plural = `${singular}s`): string {
    return `${count} ${count === 1 ? singular : plural}`;
}

// joinList for items that are not text, e.g. elements: [A, ", ", B, " and ", C].
export function interleaveListSeparators<Item>(items: Item[]): (Item | string)[] {
    return items.flatMap((item, index) => {
        if (index === 0) {
            return [item];
        }

        return [index === items.length - 1 ? " and " : ", ", item];
    });
}

// e.g. ["A", "B", "C"] -> "A, B and C".
export function joinList(items: string[]): string {
    return interleaveListSeparators(items).join("");
}

export function upperFirst(text: string): string {
    return text.charAt(0).toUpperCase() + text.slice(1);
}
