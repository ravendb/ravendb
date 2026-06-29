import { clsx, type ClassValue } from "clsx";
import { toast } from "sonner";
import { twMerge } from "tailwind-merge";

export function cn(...inputs: ClassValue[]) {
    return twMerge(clsx(inputs));
}

// Renders an ISO timestamp in the viewer's locale; falls back to the raw value
// if it isn't a parseable date.
export function formatDateTime(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}

const RELATIVE_TIME_DIVISIONS: { amount: number; unit: Intl.RelativeTimeFormatUnit }[] = [
    { amount: 60, unit: "second" },
    { amount: 60, unit: "minute" },
    { amount: 24, unit: "hour" },
    { amount: 7, unit: "day" },
    { amount: 4.34524, unit: "week" },
    { amount: 12, unit: "month" },
    { amount: Number.POSITIVE_INFINITY, unit: "year" },
];

const relativeTimeFormatter = new Intl.RelativeTimeFormat(undefined, { numeric: "auto", style: "narrow" });

// Renders an ISO timestamp as a short relative label (e.g. "19m ago", "1h ago"); falls back to the
// raw value if it isn't a parseable date.
export function formatRelativeTime(value: string) {
    const date = new Date(value);
    if (Number.isNaN(date.getTime())) {
        return value;
    }

    let duration = (date.getTime() - Date.now()) / 1000;
    for (const division of RELATIVE_TIME_DIVISIONS) {
        if (Math.abs(duration) < division.amount) {
            return relativeTimeFormatter.format(Math.round(duration), division.unit);
        }
        duration /= division.amount;
    }
    return value;
}

export async function copyToClipboard(value: string) {
    try {
        await navigator.clipboard.writeText(value);
        toast.success("Copied to clipboard");
    } catch {
        toast.error("Could not copy to clipboard");
    }
}
