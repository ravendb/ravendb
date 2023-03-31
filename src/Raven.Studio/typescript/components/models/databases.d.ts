import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;
import IndexingStatus = Raven.Client.Documents.Indexes.IndexingStatus;
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

export type DatabaseState = "Loading" | "Error" | "Offline" | "Disabled" | "Online";

export interface DatabaseSharedInfo {
    name: string;
    sharded: this is ShardedDatabaseSharedInfo;
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
}

export interface ShardedDatabaseSharedInfo extends DatabaseSharedInfo {
    shards: DatabaseSharedInfo[];
}

export type DatabaseFilterByStateOption = DatabaseState | "Sharded" | "NonSharded" | "Local" | "Remote";

export interface DatabaseFilterCriteria {
    name: string;
    states: DatabaseFilterByStateOption[];
}
