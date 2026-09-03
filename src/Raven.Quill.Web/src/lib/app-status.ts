import type { StatusTone } from "@/components/data/status-indicator";

export type StatusStyle = { tone: StatusTone; label: string };

const STATUS_STYLES: Record<string, StatusStyle> = {
    running: { tone: "positive", label: "Running" },
    warning: { tone: "warning", label: "Needs attention" },
    setup: { tone: "info", label: "Setup" },
    loading: { tone: "loading", label: "Loading" },
    failed: { tone: "danger", label: "Failed" },
    error: { tone: "danger", label: "Error" },
};

export function resolveStatusStyle(status: string): StatusStyle {
    return (
        STATUS_STYLES[status.toLowerCase()] ?? {
            tone: "muted",
            label: status ? status[0].toUpperCase() + status.slice(1) : "Unknown",
        }
    );
}
