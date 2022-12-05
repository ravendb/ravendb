import DatabasePromotionStatus = Raven.Client.ServerWide.DatabasePromotionStatus;

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
    //TODO: deleting in progress?
}
