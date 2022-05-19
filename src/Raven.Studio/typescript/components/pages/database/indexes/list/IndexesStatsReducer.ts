import { IndexCollectionProgress, IndexNodeInfo, IndexProgressInfo, IndexSharedInfo } from "../../../../models/indexes";
import { Reducer } from "react";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { produce } from "immer";
import { databaseLocationComparator } from "../../../../utils/common";
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import { WritableDraft } from "immer/dist/types/types-external";

interface ActionStatsLoaded {
    location: databaseLocationSpecifier;
    stats: IndexStats[];
    type: "StatsLoaded";
}

interface ActionProgressLoaded {
    location: databaseLocationSpecifier;
    progress: IndexProgress[];
    type: "ProgressLoaded";
}

interface ActionSetIndexPriority {
    indexName: string;
    priority: IndexPriority;
    type: "SetPriority";
}

interface ActionSetIndexLockMode {
    indexName: string;
    lockMode: IndexLockMode;
    type: "SetLockMode";
}

interface ActionDeleteIndexes {
    indexNames: string[];
    type: "DeleteIndexes";
}

interface ActionDisableIndexing {
    indexName: string;
    location: databaseLocationSpecifier;
    type: "DisableIndexing";
}

interface ActionEnableIndexing {
    indexName: string;
    location: databaseLocationSpecifier;
    type: "EnableIndexing";
}

interface ActionPauseIndexing {
    indexName: string;
    location: databaseLocationSpecifier;
    type: "PauseIndexing";
}

interface ActionResumeIndexing {
    indexName: string;
    location: databaseLocationSpecifier;
    type: "ResumeIndexing";
}

type IndexesStatsReducerAction =
    | ActionDeleteIndexes
    | ActionStatsLoaded
    | ActionProgressLoaded
    | ActionSetIndexPriority
    | ActionSetIndexLockMode
    | ActionPauseIndexing
    | ActionResumeIndexing
    | ActionDisableIndexing
    | ActionEnableIndexing;

interface IndexesStatsState {
    indexes: IndexSharedInfo[];
    locations: databaseLocationSpecifier[];
}

function mapToIndexSharedInfo(stats: IndexStats): IndexSharedInfo {
    return {
        name: stats.Name,
        collections: stats.Collections ? Object.keys(stats.Collections) : [],
        nodesInfo: [],
        lockMode: stats.LockMode,
        type: stats.Type,
        priority: stats.Priority,
        sourceType: stats.SourceType,
        reduceOutputCollectionName: stats.ReduceOutputCollection,
        patternForReferencesToReduceOutputCollection: stats.ReduceOutputReferencePattern,
        collectionNameForReferenceDocuments: stats.PatternReferencesCollectionName,
    };
}

function mapToIndexNodeInfo(stats: IndexStats, location: databaseLocationSpecifier): IndexNodeInfo {
    return {
        location,
        status: "loaded",
        details: {
            errorCount: stats.ErrorsCount,
            entriesCount: stats.EntriesCount,
            state: stats.State,
            status: stats.Status,
            stale: stats.IsStale,
            faulty: stats.Type === "Faulty",
        },
        progress: null,
    };
}

function initNodesInfo(locations: databaseLocationSpecifier[]): IndexNodeInfo[] {
    return locations.map((location) => ({
        location,
        status: "notLoaded",
        details: null,
        progress: null,
    }));
}

function markProgressAsCompleted(progress: WritableDraft<IndexProgressInfo>) {
    progress.global.processed = progress.global.total;

    progress.collections.forEach((c) => {
        c.documents.processed = c.documents.total;
        c.tombstones.processed = c.tombstones.total;
    });
}

function mapProgress(progress: IndexProgress): IndexProgressInfo {
    const collectionNames = Object.keys(progress.Collections || {});

    let grandTotal = 0;
    let grandProcessed = 0;

    const mappedCollections: IndexCollectionProgress[] = collectionNames.map((name) => {
        const stats = progress.Collections[name];
        return {
            name,
            documents: {
                processedPerSecond: 0,
                total: stats.TotalNumberOfItems,
                processed: stats.TotalNumberOfItems - stats.NumberOfItemsToProcess,
            },
            tombstones: {
                processedPerSecond: 0,
                total: stats.TotalNumberOfTombstones,
                processed: stats.TotalNumberOfTombstones - stats.NumberOfTombstonesToProcess,
            },
        };
    });

    mappedCollections.forEach((c) => {
        grandTotal += c.documents.total + c.tombstones.total;
        grandProcessed += c.documents.processed + c.tombstones.processed;
    });

    return {
        collections: mappedCollections,
        global: {
            processed: grandProcessed,
            total: grandTotal,
            processedPerSecond: progress.ProcessedPerSecond,
        },
    };
}

export const indexesStatsReducer: Reducer<IndexesStatsState, IndexesStatsReducerAction> = (
    state: IndexesStatsState,
    action: IndexesStatsReducerAction
): IndexesStatsState => {
    switch (action.type) {
        case "ProgressLoaded": {
            const incomingLocation = action.location;
            const progress = action.progress;

            return produce(state, (draft) => {
                draft.indexes.forEach((index) => {
                    const itemToUpdate = index.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );

                    const incomingProgress = progress.find((x) => x.Name === index.name);
                    if (incomingProgress) {
                        itemToUpdate.progress = mapProgress(incomingProgress);
                        if (itemToUpdate.details) {
                            itemToUpdate.details.stale = incomingProgress.IsStale;
                        }
                    } else {
                        if (itemToUpdate.progress) {
                            markProgressAsCompleted(itemToUpdate.progress);
                        }
                        if (itemToUpdate.details) {
                            itemToUpdate.details.stale = false;
                        }
                    }
                });
            });
        }
        case "StatsLoaded":
            const incomingLocation = action.location;
            const incomingStats = action.stats;

            return produce(state, (draft) => {
                incomingStats.forEach((stat) => {
                    const existingShardedInfo = draft.indexes.find((x) => x.name === stat.Name);
                    if (existingShardedInfo) {
                        // container already exists, just update node stats

                        const findIdx = existingShardedInfo.nodesInfo.findIndex((x) =>
                            databaseLocationComparator(x.location, incomingLocation)
                        );
                        if (findIdx !== -1) {
                            const nodeInfo = mapToIndexNodeInfo(stat, incomingLocation);
                            existingShardedInfo.nodesInfo.splice(findIdx, 1, nodeInfo);
                        }
                    } else {
                        // create new container with stats
                        const sharedInfo = mapToIndexSharedInfo(stat);
                        sharedInfo.nodesInfo = initNodesInfo(state.locations).map((existingNodeInfo) => {
                            if (databaseLocationComparator(existingNodeInfo.location, incomingLocation)) {
                                return mapToIndexNodeInfo(stat, incomingLocation);
                            } else {
                                return existingNodeInfo;
                            }
                        });
                        draft.indexes.push(sharedInfo);
                    }
                });
            });
        case "DeleteIndexes":
            return produce(state, (draft) => {
                draft.indexes = draft.indexes.filter((x) => !action.indexNames.includes(x.name));
            });
        case "SetPriority":
            return produce(state, (draft) => {
                const matchedIndex = draft.indexes.find((x) => x.name === action.indexName);
                matchedIndex.priority = action.priority;
            });
        case "SetLockMode":
            return produce(state, (draft) => {
                const matchedIndex = draft.indexes.find((x) => x.name === action.indexName);
                matchedIndex.lockMode = action.lockMode;
            });
        case "EnableIndexing":
            return produce(state, (draft) => {
                const index = draft.indexes.find((x) => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find((x) => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Running";
                    nodeInfo.details.state = "Normal";
                }
            });
        case "DisableIndexing":
            return produce(state, (draft) => {
                const index = draft.indexes.find((x) => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find((x) => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Disabled";
                    nodeInfo.details.state = "Disabled";
                }
            });
        case "PauseIndexing":
            return produce(state, (draft) => {
                const index = draft.indexes.find((x) => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find((x) => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Paused";
                }
            });
        case "ResumeIndexing":
            return produce(state, (draft) => {
                const index = draft.indexes.find((x) => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find((x) => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Running";
                }
            });
        default:
            console.warn("Unhandled action: ", action);
            return state;
    }
};

export const indexesStatsReducerInitializer = (locations: databaseLocationSpecifier[]): IndexesStatsState => {
    return {
        indexes: [],
        locations,
    };
};
