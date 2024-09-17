import IndexSourceType = Raven.Client.Documents.Indexes.IndexSourceType;
import { SortDirection, loadStatus } from "./common";
import SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType;

export type IndexStatus = "Normal" | "ErrorOrFaulty" | "Stale" | "Paused" | "Disabled" | "Idle" | "RollingDeployment";
export type IndexType = "StaticIndex" | "AutoIndex";

export interface IndexGroup {
    name: string;
    indexes: IndexSharedInfo[];
}

export interface IndexSharedInfo {
    name: string;
    sourceType: IndexSourceType;
    collections: string[];
    priority: Raven.Client.Documents.Indexes.IndexPriority;
    type: Raven.Client.Documents.Indexes.IndexType;
    lockMode: Raven.Client.Documents.Indexes.IndexLockMode;
    searchEngine: SearchEngineType;
    reduceOutputCollectionName: string;
    patternForReferencesToReduceOutputCollection: string;
    collectionNameForReferenceDocuments: string;
    nodesInfo: IndexNodeInfo[];
    referencedCollections: string[];
}

export interface IndexNodeInfo {
    location: databaseLocationSpecifier;
    status: loadStatus;
    details: IndexNodeInfoDetails;
    loadError?: any;
    progress: IndexProgressInfo;
    createdTimestamp: Date;
}

export interface IndexProgressInfo {
    collections: IndexCollectionProgress[];
    global: IndexingProgress;
}

export interface IndexCollectionProgress {
    name: string;
    documents: IndexingProgress;
    tombstones: IndexingProgress;
    deletedTimeSeries: IndexingProgress;
}

export interface IndexingProgress extends Progress {
    processedPerSecond: number;
}

export interface IndexNodeInfoDetails {
    errorCount: number;
    entriesCount: number;
    status: Raven.Client.Documents.Indexes.IndexRunningStatus;
    state: Raven.Client.Documents.Indexes.IndexState;
    stale: boolean;
    faulty: boolean;
    lastIndexingTime: Date;
    lastQueryingTime: Date;
}

export type IndexGroupBy = "Collection" | "None";
export type IndexSortBy =
    | Extract<keyof IndexSharedInfo, "name">
    | Extract<keyof IndexNodeInfo, "createdTimestamp">
    | Extract<keyof IndexNodeInfoDetails, "lastIndexingTime" | "lastQueryingTime">;

export interface IndexFilterCriteria {
    searchText: string;
    statuses: IndexStatus[];
    types: IndexType[];
    showOnlyIndexesWithIndexingErrors: boolean;
    autoRefresh: boolean;
    sortBy: IndexSortBy;
    sortDirection: SortDirection;
    groupBy: IndexGroupBy;
}
