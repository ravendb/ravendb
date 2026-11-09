import DatabaseUtils from "components/utils/DatabaseUtils";
import {
    BackupDecisionEvent,
    BackupDecisionFilters,
    BackupDecisionLogEntry,
    BackupDecisionRow,
} from "./backupDecisionLogTypes";

const subReasonLabels: Record<string, string> = {
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
};

const waitingPolicies = ["Startup", "BackupTime", "BackupRunning", "DatabaseLoaded"];

export const backupDecisionEvents: BackupDecisionEvent[] = [
    "Waiting",
    "HeldBack",
    "Started",
    "Failed",
    "Completed",
    "Cancelled",
    "Info",
];

export function getSubReasonLabel(subReason: string | null): string | null {
    if (subReason == null) {
        return null;
    }
    return subReasonLabels[subReason] ?? subReason;
}

export function getEventLabel(event: BackupDecisionEvent): string {
    return event === "HeldBack" ? "Held back" : event;
}

const toTime = (iso: string) => new Date(iso).getTime();

function toEvent({ Kind, Detail }: BackupDecisionLogEntry): BackupDecisionEvent {
    if (Kind !== "Policy") {
        return Kind;
    }
    return waitingPolicies.includes(Detail) ? "Waiting" : "HeldBack";
}

const isSameRun = (a: BackupDecisionLogEntry, b: BackupDecisionLogEntry) =>
    a.Kind === b.Kind && a.Detail === b.Detail && a.Reason === b.Reason;

function toDecisionRow(run: BackupDecisionLogEntry[]): BackupDecisionRow {
    const newest = run[0];
    const firstAt = run.at(-1).Time;
    const isTask = newest.Source === "Task";

    return {
        Id: `${newest.Source}-${newest.Database}-${newest.TaskId}-${newest.Kind}-${newest.Detail}-${firstAt}`,
        Event: toEvent(newest),
        SubReason: newest.Detail,
        Reason: newest.Reason,
        DatabaseName: isTask ? DatabaseUtils.shardGroupKey(newest.Database) : null,
        ShardNumber:
            isTask && DatabaseUtils.isSharded(newest.Database) ? DatabaseUtils.shardNumber(newest.Database) : null,
        TaskName: isTask ? newest.TaskName : null,
        TaskId: isTask ? newest.TaskId : null,
        FirstAt: firstAt,
        LastAt: newest.Time,
        Count: run.length,
    };
}

const byLastAtDesc = (a: BackupDecisionRow, b: BackupDecisionRow) => toTime(b.LastAt) - toTime(a.LastAt);

export function toDecisionRows(newestFirstEntries: BackupDecisionLogEntry[]): BackupDecisionRow[] {
    const runsByStream = new Map<string, BackupDecisionLogEntry[][]>();
    for (const entry of newestFirstEntries) {
        const streamKey = JSON.stringify([entry.Source, entry.Database, entry.TaskId]);
        const runs = runsByStream.get(streamKey) ?? [];
        const currentRun = runs.at(-1);
        if (currentRun && isSameRun(currentRun[0], entry)) {
            currentRun.push(entry);
        } else {
            runs.push([entry]);
        }
        runsByStream.set(streamKey, runs);
    }
    return [...runsByStream.values()].flat().map(toDecisionRow).sort(byLastAtDesc);
}

export function getNewestRowPerStream(newestFirstRows: BackupDecisionRow[]): BackupDecisionRow[] {
    const newestByStream = new Map<string, BackupDecisionRow>();
    for (const row of newestFirstRows) {
        const streamKey = JSON.stringify([row.DatabaseName, row.ShardNumber, row.TaskId]);
        if (!newestByStream.has(streamKey)) {
            newestByStream.set(streamKey, row);
        }
    }
    return [...newestByStream.values()];
}

export function mergeDecisionLogEntries(lists: BackupDecisionLogEntry[][]): BackupDecisionLogEntry[] {
    const byContent = new Map(lists.flat().map((entry) => [JSON.stringify(entry), entry]));
    return [...byContent.values()].sort((a, b) => toTime(b.Time) - toTime(a.Time));
}

export function filterDecisions(rows: BackupDecisionRow[], filters: BackupDecisionFilters): BackupDecisionRow[] {
    const reason = filters.reason.trim().toLowerCase();
    return rows.filter((row) => {
        if (filters.events.length > 0 && !filters.events.includes(row.Event)) {
            return false;
        }
        if (filters.tasks.length > 0 && (row.TaskName == null || !filters.tasks.includes(row.TaskName))) {
            return false;
        }
        if (filters.dateFrom && toTime(row.LastAt) < filters.dateFrom.getTime()) {
            return false;
        }
        if (filters.dateTo && toTime(row.LastAt) > filters.dateTo.getTime()) {
            return false;
        }
        if (!reason) {
            return true;
        }
        return [row.Reason, getSubReasonLabel(row.SubReason)].some((text) => text?.toLowerCase().includes(reason));
    });
}
