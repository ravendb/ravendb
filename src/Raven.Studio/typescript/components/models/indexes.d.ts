import IndexSourceType = Raven.Client.Documents.Indexes.IndexSourceType;
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import { loadStatus } from "./common";
import SearchEngineType = Raven.Client.Documents.Indexes.SearchEngineType;

export type IndexStatus = "Normal" | "ErrorOrFaulty" | "Stale" | "Paused" | "Disabled" | "Idle" | "RollingDeployment";

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
}

export interface IndexNodeInfo {
    location: databaseLocationSpecifier;
    status: loadStatus;
    details: IndexNodeInfoDetails;
    progress: IndexProgressInfo;
}

export interface IndexProgressInfo {
    collections: IndexCollectionProgress[];
    global: IndexingProgress;
}

export interface IndexCollectionProgress {
    name: string;
    documents: IndexingProgress;
    tombstones: IndexingProgress;
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
}

export interface IndexFilterCriteria {
    searchText: string;
    status: IndexStatus[];
    showOnlyIndexesWithIndexingErrors: boolean;
    autoRefresh: boolean; //TODO:
}
