// Chart series get a color by position from the theme's chart tokens (defined in
// index.css for both light and dark), cycling once a chart has more than five series.
const CHART_SERIES_COLORS = ["var(--chart-1)", "var(--chart-2)", "var(--chart-3)", "var(--chart-4)", "var(--chart-5)"];

export function seriesColor(index: number): string {
    return CHART_SERIES_COLORS[index % CHART_SERIES_COLORS.length];
}

// Solid fills for agent avatars. Mid-tone hues kept legible under white initials in both
// themes, picked deterministically so an agent keeps the same color across renders.
const AGENT_AVATAR_COLORS = ["#6366f1", "#0ea5e9", "#a855f7", "#14b8a6", "#f97316", "#ec4899", "#10b981", "#ef4444"];

export function agentAvatarColor(key: string): string {
    let hash = 0;
    for (let index = 0; index < key.length; index++) {
        hash = (hash * 31 + key.charCodeAt(index)) | 0;
    }
    return AGENT_AVATAR_COLORS[Math.abs(hash) % AGENT_AVATAR_COLORS.length];
}
