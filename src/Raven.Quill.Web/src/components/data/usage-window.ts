import type { UsageWindow } from "@/api/generated/server-api";
import type { WindowKey } from "@/components/data/window-tabs";

// stats.usage (GET /api/usage) binds the PascalCase UsageWindow enum and 400s on anything
// else, so translate the lowercase UI/response window keys to those exact wire values.
export const USAGE_WINDOW_BY_KEY: Record<WindowKey, UsageWindow> = {
    last24h: "Last24h",
    last7d: "Last7d",
    last30d: "Last30d",
};
