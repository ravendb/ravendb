const compactNumberFormatter = new Intl.NumberFormat("en-US", {
    notation: "compact",
    maximumFractionDigits: 1,
});

// Headline-friendly number, e.g. 1100 -> "1.1K", 18_400_000 -> "18.4M".
export function formatCompact(value: number): string {
    return compactNumberFormatter.format(value);
}
