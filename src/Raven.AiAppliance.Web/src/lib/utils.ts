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

export async function copyToClipboard(value: string) {
    try {
        await navigator.clipboard.writeText(value);
        toast.success("Copied to clipboard");
    } catch {
        toast.error("Could not copy to clipboard");
    }
}
