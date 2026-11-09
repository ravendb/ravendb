export type BackupDecisionSource = "Server" | "Task";

export type BackupDecisionKind = "Info" | "Policy" | "Started" | "Completed" | "Failed" | "Cancelled";

export interface BackupDecisionLogDetails {
    NodeTag: string;
    MaxEntriesPerLog: number;
    Queue: BackupQueueSummary;
    Databases: string[];
    Entries: BackupDecisionLogEntry[];
    TotalResults: number;
}

export interface BackupQueueSummary {
    QueueLength: number;
    TrackedTasks: number;
    TrackedDatabases: number;
    StaleTasks: number;
    RunningTasks: number;
    BlockedTasks: number;
    CurrentNumberOfRunningBackups: number;
    MaxNumberOfConcurrentBackups: number;
    RunnerFrequencyInSec: number;
    NextBackupUtc: string | null;
    NextBackupDatabase: string | null;
    NextBackupTaskName: string | null;
}

export interface BackupDecisionLogEntry {
    Time: string;
    Source: BackupDecisionSource;
    Database: string | null;
    TaskId: number | null;
    TaskName: string | null;
    Kind: BackupDecisionKind;
    Detail: string | null;
    Reason: string | null;
}

export interface BackupDecisionRow {
    Id: string;
    Event: "Started" | "Completed" | "Failed" | "Cancelled" | "Waiting" | "HeldBack" | "Info";
    SubReason: string | null;
    Reason: string | null;
    DatabaseName: string | null;
    ShardNumber: number | null;
    TaskName: string | null;
    TaskId: number | null;
    FirstAt: string;
    LastAt: string;
    Count: number;
}

export type BackupDecisionEvent = BackupDecisionRow["Event"];

export interface BackupDecisionFilters {
    reason: string;
    events: BackupDecisionEvent[];
    databases: string[];
    tasks: string[];
    dateFrom: Date | null;
    dateTo: Date | null;
}

export const emptyBackupDecisionFilters: BackupDecisionFilters = {
    reason: "",
    events: [],
    databases: [],
    tasks: [],
    dateFrom: null,
    dateTo: null,
};
