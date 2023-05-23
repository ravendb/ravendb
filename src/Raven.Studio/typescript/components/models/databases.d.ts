import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;
import IndexRunningStatus = Raven.Client.Documents.Indexes.IndexRunningStatus;

export interface NodeInfo {
    tag: string;
    nodeUrl: string;
    type: databaseGroupNodeType;
    responsibleNode: string;
    lastError?: string;
    lastStatus?: DatabasePromotionStatus;
}

export interface DatabaseLocalInfo {
    name: string;
    location: databaseLocationSpecifier;
    indexingErrors: number;
    alerts: number;
    loadError: string;
    performanceHints: number;
    indexingStatus: IndexRunningStatus;
    documentsCount: number;
    tempBuffersSize: Raven.Client.Util.Size;
    totalSize: Raven.Client.Util.Size;
    upTime?: string;
    backupInfo: Raven.Client.ServerWide.Operations.BackupInfo;
}

export interface OrchestratorLocalInfo {
    name: string;
    nodeTag: string;
    alerts: number;
    loadError: string;
    performanceHints: number;
}

export interface TopLevelDatabaseInfo {
    name: string;
    nodeTag: string;
    alerts: number;
    loadError: string;
    performanceHints: number;
}

export type MergedDatabaseState = "Loading" | "Error" | "Offline" | "Disabled" | "Online" | "Partially Online";

export interface DatabaseSharedInfo {
    name: string;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
    encrypted: boolean;
    disabled: boolean;
    indexesCount: number;
    nodes: NodeInfo[];
    dynamicNodesDistribution: boolean;
    fixOrder: boolean;
    currentNode: {
        relevant: boolean;
        isBeingDeleted: boolean;
    };
    sharded: this is ShardedDatabaseSharedInfo;
    shards?: DatabaseSharedInfo[];
}

export interface ShardedDatabaseSharedInfo extends DatabaseSharedInfo {
    shards: DatabaseSharedInfo[];
}

export type DatabaseFilterByStateOption =
    | Exclude<MergedDatabaseState, "Partially Online">
    | "Sharded"
    | "NonSharded"
    | "Local"
    | "Remote";

export interface DatabaseFilterCriteria {
    name: string;
    states: DatabaseFilterByStateOption[];
}
