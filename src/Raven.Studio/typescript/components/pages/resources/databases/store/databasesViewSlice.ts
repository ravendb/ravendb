import { createAsyncThunk, createEntityAdapter, createSlice, EntityState, PayloadAction } from "@reduxjs/toolkit";
import { DatabaseLocalInfo, OrchestratorLocalInfo, TopLevelDatabaseInfo } from "components/models/databases";
import { perNodeTagLoadStatus } from "components/models/common";
import { toDatabaseLocalInfo } from "components/common/shell/databasesSlice";
import { databaseLocationComparator } from "components/utils/common";
import genUtils from "common/generalUtils";
import { services } from "hooks/useServices";
import StudioOrchestratorState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioOrchestratorState;

export interface DatabasesViewState {
    /**
     * holds database info specific for given shard/node (document count, errors, etc)
     */
    databaseDetailedInfo: EntityState<DatabaseLocalInfo>;
    /**
     * holds orchestrator info specific for given node (alert count, performance hints count etc)
     */
    orchestratorDetailedInfo: EntityState<OrchestratorLocalInfo>;
    /**
     * Data loading status per each node (applies to both db and orchestrator info)
     */
    detailedLoadStatus: EntityState<perNodeTagLoadStatus>;
}

const selectDatabaseInfoId = (dbName: string, location: databaseLocationSpecifier) =>
    dbName + "_$$$_" + genUtils.formatLocation(location);

const selectOrchestratorInfoId = (dbName: string, nodeTag: string) => dbName + "_$$$_" + nodeTag;

const databaseInfoAdapter = createEntityAdapter<DatabaseLocalInfo>({
    selectId: (x) => selectDatabaseInfoId(x.name, x.location),
});

const orchestratorInfoAdapter = createEntityAdapter<OrchestratorLocalInfo>({
    selectId: (x) => selectOrchestratorInfoId(x.name, x.nodeTag),
});

const databaseDetailedLoadStatusAdapter = createEntityAdapter<perNodeTagLoadStatus>({
    selectId: (x) => x.nodeTag,
});

const databaseDetailedLoadStatusSelectors = databaseDetailedLoadStatusAdapter.getSelectors();
const databaseDetailedInfoSelectors = databaseInfoAdapter.getSelectors();
const orchestratorDetailedInfoSelectors = orchestratorInfoAdapter.getSelectors();

const initialState: DatabasesViewState = {
    databaseDetailedInfo: databaseInfoAdapter.getInitialState(),
    detailedLoadStatus: databaseDetailedLoadStatusAdapter.getInitialState(),
    orchestratorDetailedInfo: orchestratorInfoAdapter.getInitialState(),
};

const sliceName = "databasesView";

export const databasesViewSlice = createSlice({
    initialState,
    name: sliceName,
    reducers: {
        disabledIndexing: (state, action: PayloadAction<string>) => {
            state.databaseDetailedInfo.ids.forEach((id) => {
                const entity = state.databaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Disabled";
                }
            });
        },
        enabledIndexing: (state, action: PayloadAction<string>) => {
            state.databaseDetailedInfo.ids.forEach((id) => {
                const entity = state.databaseDetailedInfo.entities[id];
                if (entity.name === action.payload) {
                    entity.indexingStatus = "Running";
                }
            });
        },
        pausedIndexing: {
            reducer: (state, action: PayloadAction<{ databaseName: string; location: databaseLocationSpecifier }>) => {
                state.databaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.databaseDetailedInfo.entities[id];
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
                state.databaseDetailedInfo.ids.forEach((id) => {
                    const entity = state.databaseDetailedInfo.entities[id];
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

        initDetails: {
            reducer: (state, action: PayloadAction<{ nodeTags: string[] }>) => {
                databaseDetailedLoadStatusAdapter.setAll(
                    state.detailedLoadStatus,
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

                const existingInfo = orchestratorDetailedInfoSelectors.selectById(
                    state.orchestratorDetailedInfo,
                    orchestratorInfoAdapter.selectId(newEntity)
                );

                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    orchestratorInfoAdapter.setOne(state.orchestratorDetailedInfo, newEntity);
                }
            });

            action.payload.Databases.forEach((db) => {
                const newEntity = toDatabaseLocalInfo(db, nodeTag);

                const existingInfo = databaseDetailedInfoSelectors.selectById(
                    state.databaseDetailedInfo,
                    databaseInfoAdapter.selectId(newEntity)
                );
                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    databaseInfoAdapter.setOne(state.databaseDetailedInfo, newEntity);
                }
            });
        });

        builder.addCase(fetchDatabases.fulfilled, (state, action) => {
            const nodeTag = action.meta.arg;

            const loadStatusEntities = state.detailedLoadStatus.entities;

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

                const existingInfo = orchestratorDetailedInfoSelectors.selectById(
                    state.orchestratorDetailedInfo,
                    orchestratorInfoAdapter.selectId(newEntity)
                );

                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    orchestratorInfoAdapter.setOne(state.orchestratorDetailedInfo, newEntity);
                }
            });

            action.payload.Databases.forEach((db) => {
                const newEntity = toDatabaseLocalInfo(db, nodeTag);

                const existingInfo = databaseDetailedInfoSelectors.selectById(
                    state.databaseDetailedInfo,
                    databaseInfoAdapter.selectId(newEntity)
                );
                if (!existingInfo || JSON.stringify(existingInfo) !== JSON.stringify(newEntity)) {
                    databaseInfoAdapter.setOne(state.databaseDetailedInfo, newEntity);
                }
            });
        });

        builder.addCase(fetchDatabases.rejected, (state, action) => {
            databaseDetailedLoadStatusAdapter.setOne(state.detailedLoadStatus, {
                nodeTag: action.meta.arg,
                status: "failure",
            });
        });
    },
});

const fetchDatabases = createAsyncThunk(sliceName + "/fetchDatabases", async (nodeTag: string) => {
    return await services.databasesService.getDatabasesState(nodeTag);
});

const fetchDatabase = createAsyncThunk(
    sliceName + "/fetchDatabase",
    async (payload: { nodeTag: string; databaseName: string }) => {
        return await services.databasesService.getDatabaseState(payload.nodeTag, payload.databaseName);
    }
);

function databaseInfoIntoTopLevelDatabaseInfo(localInfo: DatabaseLocalInfo, nodeTag: string): TopLevelDatabaseInfo {
    return {
        name: localInfo.name,
        nodeTag,
        alerts: localInfo.alerts,
        loadError: localInfo.loadError,
        performanceHints: localInfo.performanceHints,
    };
}

function orchestratorInfoIntoTopLevelDatabaseInfo(
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

export const databasesViewSliceInternal = {
    databaseDetailedLoadStatusSelectors: databaseDetailedLoadStatusSelectors,
    orchestratorDetailedInfoSelectors: orchestratorDetailedInfoSelectors,
    databaseDetailedInfoSelectors: databaseDetailedInfoSelectors,
    databaseInfoIntoTopLevelDatabaseInfo,
    orchestratorInfoIntoTopLevelDatabaseInfo,
    fetchDatabase,
    fetchDatabases,
    selectDatabaseInfoId,
    selectOrchestratorInfoId,
};
