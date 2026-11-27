import { BackupDecisionCategory, BackupDecisionFilters, BackupDecisionLogEntry } from "./backupDecisionLogTypes";

const detailLabels: Record<string, string> = {
    Startup: "Startup",
    CpuCredits: "CPU credits",
    LowMemory: "Low memory",
    HighDirtyMemory: "High dirty memory",
    ClusterHealth: "Cluster health",
    ConcurrentBackup: "Concurrent backup",
    BackupDisabled: "Backup disabled",
    BackupRunning: "Backup running",
    ShouldRunOnThisNode: "Should Run On This Node",
    BackupTime: "Backup time",
    DatabaseExists: "Database exists",
    DatabaseLoaded: "Database loaded",
    DatabaseState: "Database state",
    NoChanges: "No changes",
    DisableOngoingTasks: "Ongoing tasks disabled",
    disabled: "Task disabled",
    "task-deleted": "Task deleted",
    delay: "Delayed",
    unloaded: "Unloaded",
    user: "Stopped by user",
};

export const backupDecisionCategories: BackupDecisionCategory[] = [
    "Waiting",
    "HeldBack",
    "Started",
    "Failed",
    "Completed",
    "Cancelled",
];

export function getDetailLabel(detail: string | null): string | null {
    if (detail == null) {
        return null;
    }
    return detailLabels[detail] ?? detail;
}

export function getEventLabel(category: BackupDecisionCategory): string {
    return category === "HeldBack" ? "Held back" : category;
}

const newestFirst = (a: BackupDecisionLogEntry, b: BackupDecisionLogEntry) =>
    new Date(b.LastTime).getTime() - new Date(a.LastTime).getTime() || b.Id - a.Id;

export function mergeDecisionLogEntries(
    current: BackupDecisionLogEntry[],
    changed: BackupDecisionLogEntry[],
    take: number
): BackupDecisionLogEntry[] {
    const byId = new Map(current.map((x) => [x.Id, x]));
    changed.forEach((x) => byId.set(x.Id, x));
    return [...byId.values()].sort(newestFirst).slice(0, take);
}

export function toDecisionLogQuery(filters: BackupDecisionFilters, take: number): string {
    const query = new URLSearchParams({ take: String(take) });
    filters.databases.forEach((x) => query.append("database", x));
    filters.taskIds.forEach((x) => query.append("taskId", x));
    filters.events.forEach((x) => query.append("category", x));
    if (filters.reason) {
        query.append("reason", filters.reason);
    }
    if (filters.dateFrom) {
        query.append("from", filters.dateFrom.toISOString());
    }
    if (filters.dateTo) {
        query.append("to", filters.dateTo.toISOString());
    }
    return query.toString();
}
