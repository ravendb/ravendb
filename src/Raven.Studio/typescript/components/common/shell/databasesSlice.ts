import { createAsyncThunk, createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import {
    DatabaseLocalInfo,
    DatabaseSharedInfo,
    OrchestratorLocalInfo,
    TopLevelDatabaseInfo,
} from "components/models/databases";
import genUtils from "common/generalUtils";
import { perNodeTagLoadStatus } from "components/models/common";
import { services } from "hooks/useServices";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseLocationComparator } from "components/utils/common";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import StudioOrchestratorState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioOrchestratorState;

export interface DatabasesState {
    /**
     * global database information - sharded between shards/nodes - i.e. name, encryption, etc
     */
    databases: EntityState<DatabaseSharedInfo>; // global database information
    /**
     * holds database info specific for given shard/node (document count, errors, etc)
     */
    localDatabaseDetailedInfo: EntityState<DatabaseLocalInfo>;
    /**
     * holds orchestrator info specific for given node (alert count, performance hints count etc)
     */
    localOrchestratorDetailedInfo: EntityState<OrchestratorLocalInfo>;
    /**
     * Data loading status per each node (applies to both db and orchestrator info)
     */
    localDetailedLoadStatus: EntityState<perNodeTagLoadStatus>;
    activeDatabase: string;
}

const databasesAdapter = createEntityAdapter<DatabaseSharedInfo>({
    selectId: (x) => x.name,
    sortComparer: (a, b) => genUtils.sortAlphaNumeric(a.name, b.name),
});

const selectDatabaseInfoId = (dbName: string, location: databaseLocationSpecifier) =>
    dbName + "_$$$_" + genUtils.formatLocation(location);

const selectOrchestratorInfoId = (dbName: string, nodeTag: string) => dbName + "_$$$_" + nodeTag;

const localDatabaseInfoAdapter = createEntityAdapter<DatabaseLocalInfo>({
    selectId: (x) => selectDatabaseInfoId(x.name, x.location),
});

const localOrchestratorInfoAdapter = createEntityAdapter<OrchestratorLocalInfo>({
    selectId: (x) => selectOrchestratorInfoId(x.name, x.nodeTag),
});

const localDatabaseDetailedLoadStatusAdapter = createEntityAdapter<perNodeTagLoadStatus>({
    selectId: (x) => x.nodeTag,
});

const databasesSelectors = databasesAdapter.getSelectors();
const localDatabaseDetailedLoadStatusSelectors = localDatabaseDetailedLoadStatusAdapter.getSelectors();
const localDatabaseDetailedInfoSelectors = localDatabaseInfoAdapter.getSelectors();
const localOrchestratorDetailedInfoSelectors = localOrchestratorInfoAdapter.getSelectors();

const initialState: DatabasesState = {
    databases: databasesAdapter.getInitialState(),
    localDatabaseDetailedInfo: localDatabaseInfoAdapter.getInitialState(),
    localDetailedLoadStatus: localDatabaseDetailedLoadStatusAdapter.getInitialState(),
    localOrchestratorDetailedInfo: localOrchestratorInfoAdapter.getInitialState(),
    activeDatabase: null,
};

const sliceName = "databases";

export const databasesSlice = createSlice({
    initialState,
    name: sliceName,
    reducers: {
        disabledIndexing: (state, action: PayloadAction<string>) => {
            state.localDatabaseDetailedInfo.ids.forEach((id) => {
                const entity = state.localDatabaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Disabled";
                }
            });
        },
        enabledIndexing: (state, action: PayloadAction<string>) => {
            state.localDatabaseDetailedInfo.ids.forEach((id) => {
                const entity = state.localDatabaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Running";
                }
            });
        },
        pausedIndexing: {
            reducer: (state, action: PayloadAction<{ databaseName: string; location: databaseLocationSpecifier }>) => {
                state.localDatabaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.localDatabaseDetailedInfo.entities[id];
                    if (
                        entity.name === action.payload.databaseName &&
                        databaseLocationComparator(entity.location, action.payload.location)
                    ) {
                        entity.indexingStatus = "Paused";
                    }
                });
            },
            prepare: (databaseName: string, location: databaseLocationSpecifier) => {
                return {
                    payload: { databaseName, location },
                };
            },
        },
        resumedIndexing: {
            reducer: (state, action: PayloadAction<{ databaseName: string; location: databaseLocationSpecifier }>) => {
                state.localDatabaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.localDatabaseDetailedInfo.entities[id];
                    if (
                        entity.name === action.payload.databaseName &&
                        databaseLocationComparator(entity.location, action.payload.location)
                    ) {
                        entity.indexingStatus = "Running";
                    }
                });
            },
            prepare: (databaseName: string, location: databaseLocationSpecifier) => {
                return {
                    payload: { databaseName, location },
                };
            },
        },
        activeDatabaseChanged: (state, action: PayloadAction<string>) => {
            state.activeDatabase = action.payload;
        },
        databasesLoaded: (state, action: PayloadAction<DatabaseSharedInfo[]>) => {
            //TODO: update in shallow mode?
            databasesAdapter.setAll(state.databases, action.payload);
        },
        initDetails: {
            reducer: (state, action: PayloadAction<{ nodeTags: string[] }>) => {
                localDatabaseDetailedLoadStatusAdapter.setAll(
                    state.localDetailedLoadStatus,
                    action.payload.nodeTags.map((tag) => ({
                        nodeTag: tag,
                        status: "idle",
                    }))
                );
            },
            prepare: (nodeTags: string[]) => {
                return {
                    payload: {
                        nodeTags,
                    },
                };
            },
        },
    },
    extraReducers: (builder) => {
        builder.addCase(fetchDatabase.fulfilled, (state, action) => {
            const { nodeTag } = action.meta.arg;

            action.payload.Orchestrators.forEach((orchestrator) => {
                const newEntity = toOrchestratorLocalInfo(orchestrator, nodeTag);

                const existingInfo = localOrchestratorDetailedInfoSelectors.selectById(
                    state.localOrchestratorDetailedInfo,
                    localOrchestratorInfoAdapter.selectId(newEntity)
                );

                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    localOrchestratorInfoAdapter.setOne(state.localOrchestratorDetailedInfo, newEntity);
                }
            });

            action.payload.Databases.forEach((db) => {
                const newEntity = toDatabaseLocalInfo(db, nodeTag);

                const existingInfo = localDatabaseDetailedInfoSelectors.selectById(
                    state.localDatabaseDetailedInfo,
                    localDatabaseInfoAdapter.selectId(newEntity)
                );
                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    localDatabaseInfoAdapter.setOne(state.localDatabaseDetailedInfo, newEntity);
                }
            });
        });

        builder.addCase(fetchDatabases.fulfilled, (state, action) => {
            const nodeTag = action.meta.arg;

            const loadStatusEntities = state.localDetailedLoadStatus.entities;

            if (loadStatusEntities[nodeTag]) {
                loadStatusEntities[nodeTag].status = "success";
            } else {
                loadStatusEntities[nodeTag] = {
                    nodeTag,
                    status: "success",
                };
            }

            action.payload.Orchestrators.forEach((orchestrator) => {
                const newEntity = toOrchestratorLocalInfo(orchestrator, nodeTag);

                const existingInfo = localOrchestratorDetailedInfoSelectors.selectById(
                    state.localOrchestratorDetailedInfo,
                    localOrchestratorInfoAdapter.selectId(newEntity)
                );

                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    localOrchestratorInfoAdapter.setOne(state.localOrchestratorDetailedInfo, newEntity);
                }
            });

            action.payload.Databases.forEach((db) => {
                const newEntity = toDatabaseLocalInfo(db, nodeTag);

                const existingInfo = localDatabaseDetailedInfoSelectors.selectById(
                    state.localDatabaseDetailedInfo,
                    localDatabaseInfoAdapter.selectId(newEntity)
                );
                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    localDatabaseInfoAdapter.setOne(state.localDatabaseDetailedInfo, newEntity);
                }
            });
        });

        builder.addCase(fetchDatabases.rejected, (state, action) => {
            localDatabaseDetailedLoadStatusAdapter.setOne(state.localDetailedLoadStatus, {
                nodeTag: action.meta.arg,
                status: "failure",
            });
        });
    },
});

function localDatabaseInfoIntoTopLevelDatabaseInfo(
    localInfo: DatabaseLocalInfo,
    nodeTag: string
): TopLevelDatabaseInfo {
    return {
        name: localInfo.name,
        nodeTag,
        alerts: localInfo.alerts,
        loadError: localInfo.loadError,
        performanceHints: localInfo.performanceHints,
    };
}

function localOrchestratorInfoIntoTopLevelDatabaseInfo(
    localInfo: OrchestratorLocalInfo,
    nodeTag: string
): TopLevelDatabaseInfo {
    return {
        name: localInfo.name,
        nodeTag,
        alerts: localInfo.alerts,
        loadError: localInfo.loadError,
        performanceHints: localInfo.performanceHints,
    };
}

function toOrchestratorLocalInfo(state: StudioOrchestratorState, nodeTag: string): OrchestratorLocalInfo {
    return {
        name: state.Name,
        nodeTag,
        alerts: state.Alerts,
        performanceHints: state.PerformanceHints,
        loadError: state.LoadError,
    };
}

export function toDatabaseLocalInfo(db: StudioDatabaseState, nodeTag: string): DatabaseLocalInfo {
    return {
        name: DatabaseUtils.shardGroupKey(db.Name),
        location: {
            nodeTag,
            shardNumber: DatabaseUtils.shardNumber(db.Name),
        },
        alerts: db.Alerts,
        loadError: db.LoadError,
        documentsCount: db.DocumentsCount,
        indexingStatus: db.IndexingStatus,
        indexingErrors: db.IndexingErrors,
        performanceHints: db.PerformanceHints,
        upTime: db.UpTime ? genUtils.timeSpanAsAgo(db.UpTime, false) : null, // we format here to avoid constant updates of UI
        backupInfo: db.BackupInfo,
        totalSize: db.TotalSize,
        tempBuffersSize: db.TempBuffersSize,
    };
}

const fetchDatabases = createAsyncThunk(sliceName + "/fetchDatabases", async (nodeTag: string) => {
    return await services.databasesService.getDatabasesState(nodeTag);
});

const fetchDatabase = createAsyncThunk(
    sliceName + "/fetchDatabase",
    async (payload: { nodeTag: string; databaseName: string }) => {
        return await services.databasesService.getDatabaseState(payload.nodeTag, payload.databaseName);
    }
);

export const databasesSliceInternal = {
    databasesSelectors,
    localDatabaseInfoIntoTopLevelDatabaseInfo,
    localDatabaseDetailedLoadStatusSelectors,
    localOrchestratorDetailedInfoSelectors,
    selectOrchestratorInfoId,
    localOrchestratorInfoIntoTopLevelDatabaseInfo,
    selectDatabaseInfoId,
    localDatabaseDetailedInfoSelectors,
    fetchDatabases,
    fetchDatabase,
};
