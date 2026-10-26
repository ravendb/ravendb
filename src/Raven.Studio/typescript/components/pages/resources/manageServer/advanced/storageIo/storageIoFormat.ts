import genUtils from "common/generalUtils";

export function formatMs(ms: number | null | undefined): string {
    if (ms == null) {
        return "-";
    }
    if (ms >= 10_000) {
        return `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 0 })} s`;
    }
    if (ms >= 1000) {
        return `${(ms / 1000).toLocaleString(undefined, { maximumFractionDigits: 1 })} s`;
    }
    return `${ms.toLocaleString(undefined, { maximumFractionDigits: ms < 10 ? 1 : 0 })} ms`;
}

export function formatBytes(bytes: number | null | undefined): string {
    return bytes == null ? "-" : genUtils.formatBytesToSize(bytes, 1);
}

export function formatPercent(share: number | null | undefined): string {
    return share == null ? "-" : `${Math.round(share * 100)}%`;
}

export function formatAgo(isoDate: string | null | undefined, now: number = Date.now()): string {
    if (!isoDate) {
        return "-";
    }
    const seconds = Math.max(0, (now - new Date(isoDate).getTime()) / 1000);
    if (seconds < 60) {
        return `${Math.round(seconds)} s ago`;
    }
    if (seconds < 3600) {
        return `${Math.round(seconds / 60)} min ago`;
    }
    if (seconds < 48 * 3600) {
        return `${Math.round(seconds / 3600)} h ago`;
    }
    return `${Math.round(seconds / 86400)} days ago`;
}
