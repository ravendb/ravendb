import type { QuillApplicationUsage } from "@/api/generated/server-api";

// The licence server reports one row per database, keyed by topology id - and the same name can
// arrive many times over: one licence can cover several appliances, and re-provisioning one reports
// under a fresh topology id. Stacking those as rows that read identically tells the user nothing,
// so same-named rows collapse into a group that carries the shared name, a count and their combined
// usage, and expanding it identifies each row by the topology id that actually distinguishes them.
export type UsageGroup = {
    key: string;
    label: string;
    isSystem: boolean;
    // True when the group has rows worth naming individually. Every appliance has exactly one config
    // database, so the system group is expandable even at a count of one: its label deliberately
    // hides which database it is, and expanding is the only way back to that.
    isExpandable: boolean;
    rows: QuillApplicationUsage[];
    usage: number;
};

export const SYSTEM_GROUP_KEY = "system";

export const SYSTEM_GROUP_LABEL = "System";

export const SYSTEM_GROUP_DESCRIPTION =
    "Quill's own storage for your apps, channels and API keys. It is written to when you change " +
    "configuration, and those writes count toward your total.";

export function rowKey(row: QuillApplicationUsage) {
    return `${row.topologyId}/${row.applicationName}`;
}

// Descending usage, with the topology id breaking ties so equal rows can't shuffle between renders.
function byUsageDescending(a: QuillApplicationUsage, b: QuillApplicationUsage) {
    return b.usage - a.usage || a.topologyId.localeCompare(b.topologyId);
}

function toGroup(key: string, label: string, isSystem: boolean, rows: QuillApplicationUsage[]): UsageGroup {
    return {
        key,
        label,
        isSystem,
        isExpandable: isSystem || rows.length > 1,
        rows: [...rows].sort(byUsageDescending),
        usage: rows.reduce((total, row) => total + row.usage, 0),
    };
}

// Apps first, in the order the licence server listed them, then the single system group - the
// appliance's own storage is not something the user made, so it doesn't compete for the top rows.
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

    const groups = [...byName].map(([name, rows]) => toGroup(`app/${name}`, name, false, rows));

    if (system.length > 0) groups.push(toGroup(SYSTEM_GROUP_KEY, SYSTEM_GROUP_LABEL, true, system));

    return groups;
}
