export type BackupDecisionSource = "Server" | "Task";

export type BackupDecisionCategory = "Waiting" | "HeldBack" | "Started" | "Completed" | "Failed" | "Cancelled";

export interface BackupDecisionLogEntry {
    Id: number;
    FirstTime: string;
    LastTime: string;
    Count: number;
    Category: BackupDecisionCategory;
    Detail: string | null;
    Reason: string | null;
    Source: BackupDecisionSource;
    Database: string | null;
    Shard: number | null;
    TaskId: number | null;
    TaskName: string | null;
    NodeTag: string;
}

export interface BackupDecisionLogTask {
    Database: string;
    TaskId: number;
    TaskName: string;
}

export interface BackupDecisionOutcome {
    Database: string;
    TaskId: number;
    TaskName: string;
    Time: string;
    Error: string | null;
}

export interface BackupDecisionLogSummary {
    Running: {
        Current: number;
        Max: number;
        WaitingForSlot: number;
    };
    WaitingForSchedule: {
        Count: number;
        NextBackupUtc: string | null;
        NextBackupDatabase: string | null;
        NextBackupTaskName: string | null;
    };
    HeldBack: {
        Count: number;
        Longest: {
            Database: string;
            TaskId: number;
            TaskName: string;
            Detail: string | null;
            Reason: string | null;
            Since: string;
        } | null;
    };
    ServerBlock: {
        Detail: string | null;
        Reason: string | null;
        Since: string;
    } | null;
    Failed: {
        Count: number;
        Latest: BackupDecisionOutcome | null;
    };
    Completed: {
        Count: number;
        Latest: BackupDecisionOutcome | null;
    };
}

export interface BackupDecisionLogDetails {
    NodeTag: string;
    RunnerStartTime: string;
    Summary: BackupDecisionLogSummary;
    Entries: BackupDecisionLogEntry[];
    TotalResults: number;
    Databases: string[] | null;
    Tasks: BackupDecisionLogTask[] | null;
}

export interface BackupDecisionLogMessage extends BackupDecisionLogDetails {
    Type: "Snapshot" | "Update";
}

export interface BackupDecisionFilters {
    reason: string;
    events: BackupDecisionCategory[];
    databases: string[];
    taskIds: string[];
    dateFrom: Date | null;
    dateTo: Date | null;
}

export const emptyBackupDecisionFilters: BackupDecisionFilters = {
    reason: "",
    events: [],
    databases: [],
    taskIds: [],
    dateFrom: null,
    dateTo: null,
};
