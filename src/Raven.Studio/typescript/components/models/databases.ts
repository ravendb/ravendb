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
    databaseStatus: Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseStatus;
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

export interface NonShardedDatabaseInfo {
    name: string;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
    isEncrypted: boolean;
    isDisabled: boolean;
    indexesCount: number;
    nodes: NodeInfo[];
    isDynamicNodesDistribution: boolean;
    isFixOrder: boolean;
    currentNode: {
        isRelevant: boolean;
        isBeingDeleted: boolean;
    };
    isSharded: false;
    environment: Raven.Client.Documents.Operations.Configuration.StudioConfiguration.StudioEnvironment;
}

export interface ShardedDatabaseInfo extends Omit<NonShardedDatabaseInfo, "isSharded"> {
    isSharded: true;
    shards: NonShardedDatabaseInfo[];
}

export type DatabaseSharedInfo = NonShardedDatabaseInfo | ShardedDatabaseInfo;

export type DatabaseFilterByStateOption = Exclude<MergedDatabaseState, "Partially Online"> | "Local" | "Remote";

export interface DatabaseFilterCriteria {
    name: string;
    states: DatabaseFilterByStateOption[];
}
