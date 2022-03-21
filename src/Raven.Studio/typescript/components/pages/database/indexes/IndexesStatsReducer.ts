import { IndexNodeInfo, IndexSharedInfo } from "../../../models/indexes";
import { Reducer } from "react";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;
import { produce } from "immer";
import { databaseLocationComparator } from "../../../utils/common";

interface ActionStatsLoaded {
    location: databaseLocationSpecifier;
    stats: IndexStats[];
    type: "StatsLoaded";
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
    type: "DeleteIndexes"
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
    ActionDeleteIndexes
    | ActionStatsLoaded
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
        collections: Object.keys(stats.Collections),
        nodesInfo: [],
        lockMode: stats.LockMode,
        type: stats.Type,
        priority: stats.Priority,
        sourceType: stats.SourceType,
        reduceOutputCollectionName: stats.ReduceOutputCollection,
        patternForReferencesToReduceOutputCollection: stats.ReduceOutputReferencePattern,
        collectionNameForReferenceDocuments: stats.PatternReferencesCollectionName
    }
}

function mapToIndexNodeInfo(stats: IndexStats, location: databaseLocationSpecifier): IndexNodeInfo {
    return {
        location,
        status: "loaded",
        details: {
            errorCount: stats.ErrorsCount,
            entriesCount: stats.EntriesCount,
            state: stats.State,
            status: stats.Status
        }
    }
}

function initNodesInfo(locations: databaseLocationSpecifier[]): IndexNodeInfo[] {
    return locations.map(location => ({
        location,
        status: "notLoaded",
        details: null
    }));
}

export const indexesStatsReducer: Reducer<IndexesStatsState, IndexesStatsReducerAction> = (state: IndexesStatsState, action: IndexesStatsReducerAction): IndexesStatsState => {
    switch (action.type) {
        case "StatsLoaded":
            const incomingLocation = action.location;
            const indexes = action.stats.map((incomingStats): IndexSharedInfo => {
                const existingSharedInfo = state.indexes.find(x => x.name === incomingStats.Name);

                if (existingSharedInfo) {
                    // container already exists, just update node stats
                    
                    return {
                        ...existingSharedInfo,
                        
                        nodesInfo: existingSharedInfo.nodesInfo.map(existingNodeInfo => {
                            if (existingNodeInfo.location.nodeTag === incomingLocation.nodeTag && existingNodeInfo.location.shardNumber === incomingLocation.shardNumber) {
                                return mapToIndexNodeInfo(incomingStats, incomingLocation);
                            } else {
                                return existingNodeInfo;
                            }
                        })
                    }
                } else {
                    // create new container with stats
                    const sharedInfo = mapToIndexSharedInfo(incomingStats);
                    sharedInfo.nodesInfo = initNodesInfo(state.locations).map(existingNodeInfo => {
                        if (existingNodeInfo.location.nodeTag === incomingLocation.nodeTag && existingNodeInfo.location.shardNumber === incomingLocation.shardNumber) {
                            return mapToIndexNodeInfo(incomingStats, incomingLocation);
                        } else {
                            return existingNodeInfo;
                        }
                    });
                    
                    return sharedInfo;
                }
            });
            
            return {
                ...state,
                indexes
            }
        case "DeleteIndexes":
            return produce(state, draft => {
                draft.indexes = draft.indexes.filter(x => !action.indexNames.includes(x.name));
            });
        case "SetPriority":
            return produce(state, draft => {
                const matchedIndex = draft.indexes.find(x => x.name === action.indexName);
                matchedIndex.priority = action.priority;
            });
        case "SetLockMode":
            return produce(state, draft => {
                const matchedIndex = draft.indexes.find(x => x.name === action.indexName);
                matchedIndex.lockMode = action.lockMode;
            });
        case "EnableIndexing":
            return produce(state, draft => {
                const index = draft.indexes.find(x => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find(x => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Running";
                    nodeInfo.details.state = "Normal";
                }
            });
        case "DisableIndexing":
            return produce(state, draft => {
                const index = draft.indexes.find(x => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find(x => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Disabled";
                    nodeInfo.details.state = "Disabled";    
                }
            });
        case "PauseIndexing":
            return produce(state, draft => {
                const index = draft.indexes.find(x => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find(x => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Paused";
                }
            });
        case "ResumeIndexing":
            return produce(state, draft => {
                const index = draft.indexes.find(x => x.name === action.indexName);
                const nodeInfo = index.nodesInfo.find(x => databaseLocationComparator(x.location, action.location));
                if (nodeInfo.details) {
                    nodeInfo.details.status = "Running";
                }
            });
        default:
            console.warn("Unhandled action: ", action)
            return state;
    }
}

export const indexesStatsReducerInitializer = (locations: databaseLocationSpecifier[]): IndexesStatsState => {
    return {
        indexes: [],
        locations
    }
}
