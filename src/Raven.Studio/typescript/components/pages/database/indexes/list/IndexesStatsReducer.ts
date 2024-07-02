import { IndexCollectionProgress, IndexNodeInfo, IndexProgressInfo, IndexSharedInfo } from "components/models/indexes";
import { Reducer } from "react";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { produce, Draft } from "immer";
import { databaseLocationComparator } from "components/utils/common";
import IndexProgress = Raven.Client.Documents.Indexes.IndexProgress;
import genUtils = require("common/generalUtils");

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

interface ActionProgressLoadError {
    location: databaseLocationSpecifier;
    error: JQueryXHR;
    type: "ProgressLoadError";
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

interface ActionResetIndex {
    type: "ResetIndex";
    location: databaseLocationSpecifier;
    indexName: string;
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

interface ActionLocationsLoaded {
    locations: databaseLocationSpecifier[];
    type: "LocationsLoaded";
}

type IndexesStatsReducerAction =
    | ActionDeleteIndexes
    | ActionStatsLoaded
    | ActionProgressLoadError
    | ActionProgressLoaded
    | ActionSetIndexPriority
    | ActionSetIndexLockMode
    | ActionPauseIndexing
    | ActionResumeIndexing
    | ActionResetIndex
    | ActionDisableIndexing
    | ActionEnableIndexing
    | ActionLocationsLoaded;

interface IndexesStatsState {
    indexes: IndexSharedInfo[];
    locations: databaseLocationSpecifier[];
    resetInProgress: string[];
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
        searchEngine: stats.SearchEngineType,
        referencedCollections: stats.ReferencedCollections,
    };
}

function mapToIndexNodeInfo(stats: IndexStats, location: databaseLocationSpecifier): IndexNodeInfo {
    return {
        location,
        status: "success",
        details: {
            errorCount: stats.ErrorsCount,
            entriesCount: stats.EntriesCount,
            state: stats.State,
            status: stats.Status,
            stale: stats.IsStale,
            faulty: stats.Type === "Faulty",
            lastIndexingTime: stats.LastIndexingTime ? new Date(stats.LastIndexingTime) : null,
            lastQueryingTime: stats.LastQueryingTime ? new Date(stats.LastQueryingTime) : null,
        },
        progress: null,
        createdTimestamp: genUtils.isServerMinDate(stats.CreatedTimestamp) ? null : new Date(stats.CreatedTimestamp),
    };
}

function initNodesInfo(locations: databaseLocationSpecifier[]): IndexNodeInfo[] {
    return locations.map((location) => ({
        location,
        status: "idle",
        details: null,
        progress: null,
        createdTimestamp: null,
    }));
}

function markProgressAsCompleted(progress: Draft<IndexProgressInfo>) {
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
        case "ProgressLoadError": {
            const incomingLocation = action.location;
            const error = action.error;

            return produce(state, (draft) => {
                draft.indexes.forEach((index) => {
                    const nodeInfo = index.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );

                    nodeInfo.status = "failure";
                    nodeInfo.details = null;
                    nodeInfo.loadError = error;
                });
            });
        }
        case "StatsLoaded": {
            const incomingLocation = action.location;
            const incomingStats = action.stats;

            return produce(state, (draft) => {
                const localIndexes: string[] = draft.indexes.map((x) => x.name);
                const incomingIndexes = incomingStats.map((x) => x.Name);
                const toDelete = localIndexes.filter(
                    (x) => !incomingIndexes.includes(x) && !draft.resetInProgress.includes(x)
                );

                if (draft.resetInProgress.length > 0) {
                    draft.resetInProgress = draft.resetInProgress.filter((x) => !incomingIndexes.includes(x));
                }

                incomingStats.forEach((stat) => {
                    const existingShardedInfo = draft.indexes.find((x) => x.name === stat.Name);
                    if (existingShardedInfo) {
                        // container already exists, just update stats

                        const sharedInfo: IndexSharedInfo = {
                            ...mapToIndexSharedInfo(stat),
                            nodesInfo: existingShardedInfo.nodesInfo,
                        };

                        const nodeInfo = mapToIndexNodeInfo(stat, incomingLocation);
                        const findIdx = sharedInfo.nodesInfo.findIndex((x) =>
                            databaseLocationComparator(x.location, incomingLocation)
                        );

                        if (findIdx === -1) {
                            sharedInfo.nodesInfo.push(nodeInfo);
                        } else {
                            nodeInfo.progress = sharedInfo.nodesInfo[findIdx].progress;
                            sharedInfo.nodesInfo.splice(findIdx, 1, nodeInfo);
                        }

                        draft.indexes.splice(draft.indexes.indexOf(existingShardedInfo), 1, sharedInfo);
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

                if (toDelete.length > 0) {
                    draft.indexes = draft.indexes.filter((x) => !toDelete.includes(x.name));
                }
            });
        }
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
        case "ResetIndex":
            return produce(state, (draft) => {
                if (!draft.resetInProgress.includes(action.indexName)) {
                    draft.resetInProgress.push(action.indexName);
                }
                const index = draft.indexes.find((x) => x.name === action.indexName);
                if (index) {
                    const nodeInfo = index.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, action.location)
                    );
                    if (nodeInfo) {
                        nodeInfo.details.stale = true;
                        if (nodeInfo.progress) {
                            nodeInfo.progress.global.processed = 0;
                        }
                    }
                }
            });
        case "LocationsLoaded":
            return produce(state, (draft) => {
                draft.locations = action.locations;

                const allNodeInfoLocations = draft.indexes.map((x) => x.nodesInfo.map((y) => y.location)).flat();
                const uniqueNodeInfoLocations = _.uniqWith(allNodeInfoLocations, databaseLocationComparator);

                for (const uniqueNodeInfoLocation of uniqueNodeInfoLocations) {
                    const existingNodeInfoLocations = draft.locations.find((currentLocation) =>
                        databaseLocationComparator(currentLocation, uniqueNodeInfoLocation)
                    );

                    if (!existingNodeInfoLocations) {
                        draft.indexes.forEach((index) => {
                            index.nodesInfo = index.nodesInfo.filter(
                                (x) => !databaseLocationComparator(x.location, uniqueNodeInfoLocation)
                            );
                        });
                    }
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
        resetInProgress: [],
    };
};
