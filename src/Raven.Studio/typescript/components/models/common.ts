import { ReactNode } from "react";

export type loadStatus = "idle" | "loading" | "success" | "failure";

export type TextColor =
    | "primary"
    | "secondary"
    | "success"
    | "info"
    | "warning"
    | "danger"
    | "muted"
    | "emphasis"
    | "node"
    | "shard"
    | "orchestrator";

export interface loadableData<T> {
    data?: T;
    status: loadStatus;
    error?: any;
}

export interface locationAwareLoadableData<T> extends loadableData<T> {
    location: databaseLocationSpecifier;
}

export interface perLocationLoadStatus {
    location: databaseLocationSpecifier;
    status: loadStatus;
}

export interface perNodeTagLoadStatus {
    nodeTag: string;
    status: loadStatus;
}

export interface InputItemLimit {
    value: number;
    badgeColor?: TextColor;
    message?: ReactNode | ReactNode[];
}

export interface InputItem<T extends string | number = string> {
    label: string;
    value: T;
    count?: number;
    limit?: InputItemLimit;
    verticalSeparatorLine?: boolean;
}

export type SortDirection = "asc" | "desc";

export interface ClassNameProps {
    className?: string;
}

export interface RestorePoint {
    dateTime?: string;
    location?: string;
    fileName?: string;
    isSnapshotRestore?: boolean;
    isIncremental?: boolean;
    isEncrypted?: boolean;
    filesToRestore?: number;
    databaseName?: string;
    nodeTag?: string;
    backupType?: string;
}

export type SelectionState = "AllSelected" | "SomeSelected" | "Empty";
