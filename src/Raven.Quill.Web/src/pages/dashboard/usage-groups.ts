import type { QuillApplicationUsage } from "@/api/generated/server-api";

// One licence can cover several appliances, and re-provisioning one reports under a fresh topology
// id, so the same name arrives many times over. Same-named rows collapse into a group; the topology
// ids behind it are what tell them apart.
export type UsageGroup = {
    key: string;
    label: string;
    isExpandable: boolean;
    rows: QuillApplicationUsage[];
    usage: number;
};

export function rowKey(row: QuillApplicationUsage) {
    return `${row.topologyId}/${row.applicationName}`;
}

// Topology id breaks ties so equal rows can't shuffle between renders.
function byUsageDescending(a: QuillApplicationUsage, b: QuillApplicationUsage) {
    return b.usage - a.usage || a.topologyId.localeCompare(b.topologyId);
}

function toGroup(name: string, rows: QuillApplicationUsage[]): UsageGroup {
    return {
        key: `app/${name}`,
        label: name,
        isExpandable: rows.length > 1,
        rows: rows.toSorted(byUsageDescending),
        usage: rows.reduce((total, row) => total + row.usage, 0),
    };
}

// Ordered by usage descending.
export function toUsageGroups(apps: QuillApplicationUsage[]): UsageGroup[] {
    const byName = new Map<string, QuillApplicationUsage[]>();

    for (const app of apps) {
        const existing = byName.get(app.applicationName);
        if (existing) existing.push(app);
        else byName.set(app.applicationName, [app]);
    }

    return Array.from(byName, ([name, rows]) => toGroup(name, rows)).sort(
        (a, b) => b.usage - a.usage || a.label.localeCompare(b.label),
    );
}
