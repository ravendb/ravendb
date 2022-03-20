import { IndexNodeInfo, IndexSharedInfo } from "../../../models/indexes";
import { Reducer } from "react";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import IndexPriority = Raven.Client.Documents.Indexes.IndexPriority;
import IndexLockMode = Raven.Client.Documents.Indexes.IndexLockMode;

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

type IndexesStatsReducerAction = ActionStatsLoaded | ActionSetIndexPriority | ActionSetIndexLockMode | ActionDisableIndexing | ActionEnableIndexing;

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
        case "SetPriority":
            return {
                ...state,
                indexes: state.indexes.map(index => {
                    if (index.name === action.indexName) {
                        return {
                            ...index,
                            priority: action.priority
                        }
                    }
                    return index;
                })
            }
        case "SetLockMode":
            return {
                ...state,
                indexes: state.indexes.map(index => {
                    if (index.name === action.indexName) {
                        return {
                            ...index,
                            lockMode: action.lockMode
                        }
                    }
                    return index;
                })
            }
        case "EnableIndexing":
            return {
                ...state,
                indexes: state.indexes.map(index => {
                    if (index.name !== action.indexName) {
                        return index;
                    } else {
                        return {
                            ...index,
                            nodesInfo: index.nodesInfo.map(nodeInfo => {
                                if (nodeInfo.location.nodeTag === action.location.nodeTag && nodeInfo.location.shardNumber === action.location.shardNumber) {
                                    return {
                                        ...nodeInfo,
                                        details: {
                                            ...nodeInfo.details,
                                            state: "Normal",
                                            status: "Running"
                                        }
                                    }
                                }
                                return nodeInfo;
                            })
                        }
                    }
                })
            }
        case "DisableIndexing":
            return {
                ...state,
                indexes: state.indexes.map(index => {
                    if (index.name !== action.indexName) {
                        return index;
                    } else {
                        return {
                            ...index,
                            nodesInfo: index.nodesInfo.map(nodeInfo => {
                                if (nodeInfo.location.nodeTag === action.location.nodeTag && nodeInfo.location.shardNumber === action.location.shardNumber) {
                                    return {
                                        ...nodeInfo,
                                        details: {
                                            ...nodeInfo.details,
                                            state: "Disabled",
                                            status: "Disabled"
                                        }
                                    }
                                }
                                return nodeInfo;
                            })
                        }
                    }
                })
            }
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
