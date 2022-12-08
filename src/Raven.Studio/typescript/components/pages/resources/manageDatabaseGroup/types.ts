import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;

export interface DatabaseInfoLoaded {
    info: Raven.Client.ServerWide.Operations.DatabaseInfo;
    type: "DatabaseInfoLoaded";
}

export interface NodeInfo {
    tag: string;
    serverUrl: string;
    responsibleNode: string;
    type: databaseGroupNodeType;

    lastStatus?: DatabasePromotionStatus;
    lastError?: string;
}

export interface ManageDatabaseGroupState {
    nodes: NodeInfo[];
    deletionInProgress: string[];
    encrypted: boolean;
    dynamicDatabaseDistribution: boolean;
    priorityOrder: string[];
    lockMode: DatabaseLockMode;
    fixOrder: boolean;
}
