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
    sharded: boolean;
    lockMode: DatabaseLockMode;
    deletionInProgress: string[];
    encrypted: boolean;
    nodes: NodeInfo[];
    currentNode: {
        relevant: boolean;
        disabled: boolean;
        isBeingDeleted: boolean;
    };
}

export interface DatabaseFilterCriteria {
    searchText: string;
}
