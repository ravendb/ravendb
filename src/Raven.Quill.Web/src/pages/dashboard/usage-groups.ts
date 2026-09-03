import type { QuillApplicationUsage } from "@/api/generated/server-api";

// One licence can cover several appliances, and re-provisioning one reports under a fresh topology
// id, so the same name arrives many times over. Same-named rows collapse into a group; the topology
// ids behind it are what tell them apart.
export type UsageGroup = {
    key: string;
    label: string;
    isSystem: boolean;
    // The system group expands even at a count of one: its label hides which database it is, and
    // expanding is the only way back to that.
    isExpandable: boolean;
    rows: QuillApplicationUsage[];
    usage: number;
};

export const SYSTEM_GROUP_KEY = "system";

export const SYSTEM_GROUP_LABEL = "@system";

export const SYSTEM_GROUP_DESCRIPTION =
    "Quill's own storage for your apps, channels and API keys. It is written to when you change " +
    "configuration, and those writes count toward your total.";

export function rowKey(row: QuillApplicationUsage) {
    return `${row.topologyId}/${row.applicationName}`;
}

// Topology id breaks ties so equal rows can't shuffle between renders.
function byUsageDescending(a: QuillApplicationUsage, b: QuillApplicationUsage) {
    return b.usage - a.usage || a.topologyId.localeCompare(b.topologyId);
}

function toGroup(key: string, label: string, isSystem: boolean, rows: QuillApplicationUsage[]): UsageGroup {
    return {
        key,
        label,
        isSystem,
        isExpandable: isSystem || rows.length > 1,
        rows: rows.toSorted(byUsageDescending),
        usage: rows.reduce((total, row) => total + row.usage, 0),
    };
}

// Apps first, ordered by usage descending, then the system group last.
export function toUsageGroups(apps: QuillApplicationUsage[]): UsageGroup[] {
    const byName = new Map<string, QuillApplicationUsage[]>();
    const system: QuillApplicationUsage[] = [];

    for (const app of apps) {
        if (app.isSystem) {
            system.push(app);
            continue;
        }

        const existing = byName.get(app.applicationName);
        if (existing) existing.push(app);
        else byName.set(app.applicationName, [app]);
    }

    const groups = Array.from(byName, ([name, rows]) => toGroup(`app/${name}`, name, false, rows)).sort(
        (a, b) => b.usage - a.usage || a.label.localeCompare(b.label),
    );

    if (system.length > 0) groups.push(toGroup(SYSTEM_GROUP_KEY, SYSTEM_GROUP_LABEL, true, system));

    return groups;
}
