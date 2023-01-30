import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;

export interface NodeInfo {
    tag: string;
    nodeUrl: string;
    type: databaseGroupNodeType;
    responsibleNode: string;
    lastError?: string;
    lastStatus?: DatabasePromotionStatus;
}

export interface DatabaseSharedInfo {
    name: string;
    sharded: this is ShardedDatabaseSharedInfo;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
    encrypted: boolean;
    disabled: boolean;
    nodes: NodeInfo[];
    currentNode: {
        relevant: boolean;
        isBeingDeleted: boolean;
    };
}

export interface ShardedDatabaseSharedInfo extends DatabaseSharedInfo {
    shards: DatabaseSharedInfo[];
}

export interface DatabaseFilterCriteria {
    searchText: string;
}
