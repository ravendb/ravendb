import type { StatusTone } from "@/components/data/status-indicator";

export type StatusStyle = { tone: StatusTone; label: string };

// Maps the server's derived status codes (MetricsReadService.DeriveAppStatus emits
// running/warning/setup) onto the app's status vocabulary. loading/failed are kept for
// forward-compatibility so a future provisioning/error state renders sensibly. This is the
// single source of truth shared by the dashboard table and the data-source detail header.
const STATUS_STYLES: Record<string, StatusStyle> = {
    running: { tone: "positive", label: "Healthy" },
    healthy: { tone: "positive", label: "Healthy" },
    warning: { tone: "warning", label: "Needs attention" },
    setup: { tone: "info", label: "Setup" },
    loading: { tone: "loading", label: "Loading" },
    failed: { tone: "danger", label: "Failed" },
    error: { tone: "danger", label: "Failed" },
};

export function resolveStatusStyle(status: string): StatusStyle {
    return (
        STATUS_STYLES[status.toLowerCase()] ?? {
            tone: "muted",
            label: status ? status[0].toUpperCase() + status.slice(1) : "Unknown",
        }
    );
}
