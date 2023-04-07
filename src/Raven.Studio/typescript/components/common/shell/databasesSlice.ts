import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    EntityState,
    PayloadAction,
    UnsubscribeListener,
} from "@reduxjs/toolkit";
import {
    DatabaseFilterByStateOption,
    DatabaseFilterCriteria,
    DatabaseLocalInfo,
    DatabaseSharedInfo,
    OrchestratorLocalInfo,
    ShardedDatabaseSharedInfo,
    TopLevelDatabaseInfo,
} from "components/models/databases";
import genUtils from "common/generalUtils";
import { AppAsyncThunk, AppDispatch, AppThunk, RootState } from "components/store";
import createDatabase from "viewmodels/resources/createDatabase";
import app from "durandal/app";
import deleteDatabaseConfirm from "viewmodels/resources/deleteDatabaseConfirm";
import DatabaseLockMode = Raven.Client.ServerWide.DatabaseLockMode;
import disableDatabaseToggleConfirm from "viewmodels/resources/disableDatabaseToggleConfirm";
import viewHelpers from "common/helpers/view/viewHelpers";
import changesContext from "common/changesContext";
import compactDatabaseDialog from "viewmodels/resources/compactDatabaseDialog";
import databasesManager from "common/shell/databasesManager";
import { InputItem, locationAwareLoadableData, perNodeTagLoadStatus } from "components/models/common";
import { services } from "hooks/useServices";
import DatabaseUtils from "components/utils/DatabaseUtils";
import { databaseLocationComparator } from "components/utils/common";
import disableIndexingToggleConfirm from "viewmodels/resources/disableIndexingToggleConfirm";
import notificationCenter from "common/notifications/notificationCenter";
import assertUnreachable from "components/utils/assertUnreachable";
import StudioDatabaseState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioDatabaseState;
import { addAppListener } from "components/storeUtils";
import { selectClusterNodeTags, selectLocalNodeTag } from "components/common/shell/clusterSlice";
import StudioOrchestratorState = Raven.Server.Web.System.Processors.Studio.StudioDatabasesHandlerForGetDatabasesState.StudioOrchestratorState;

interface DatabasesState {
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
    searchCriteria: DatabaseFilterCriteria;
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
    searchCriteria: {
        name: "",
        states: [],
    },
};

const sliceName = "databases";

export const selectAllDatabases = (store: RootState) => databasesSelectors.selectAll(store.databases.databases);

export const selectAllDatabasesCount = (store: RootState) => databasesSelectors.selectTotal(store.databases.databases);

export const selectDatabaseSearchCriteria = (store: RootState) => store.databases.searchCriteria;

export const selectFilterByStateOptions = (store: RootState): InputItem<DatabaseFilterByStateOption>[] => {
    let error = 0,
        offline = 0,
        disabled = 0,
        online = 0,
        sharded = 0,
        nonSharded = 0,
        local = 0,
        remote = 0;

    selectAllDatabases(store).forEach((db) => {
        const perNodeState = selectDatabaseState(db.name)(store);
        const state = DatabaseUtils.getDatabaseState(db, perNodeState);

        switch (state) {
            case "Loading":
                break;
            case "Error":
                error++;
                break;
            case "Offline":
                offline++;
                break;
            case "Disabled":
                disabled++;
                break;
            case "Online":
                online++;
                break;
            default:
                assertUnreachable(state);
        }

        if (db.sharded) {
            sharded++;
        } else {
            nonSharded++;
        }

        if (db.currentNode.relevant) {
            local++;
        } else {
            remote++;
        }
    });

    const localNodeTag = selectLocalNodeTag(store);

    return [
        { value: "Online", label: "Online", count: online },
        { value: "Offline", label: "Offline", count: offline },
        { value: "Error", label: "Errored", count: error },
        { value: "Disabled", label: "Disabled", count: disabled },
        { value: "Sharded", label: "Sharded", count: sharded, verticalSeparatorLine: true },
        { value: "NonSharded", label: "Non Sharded", count: nonSharded },
        {
            value: "Local",
            label: `Local (Node ${localNodeTag})`,
            count: local,
            verticalSeparatorLine: true,
        },
        { value: "Remote", label: "Remote", count: remote },
    ];
};

const isDatabaseInFilterState = (
    store: RootState,
    db: DatabaseSharedInfo,
    filterStates: DatabaseFilterByStateOption[]
): boolean => {
    const perNodeState = selectDatabaseState(db.name)(store);
    const databaseState = DatabaseUtils.getDatabaseState(db, perNodeState);

    // prettier-ignore
    return (
        (
            !filterStates.some((x) => ["Online", "Offline", "Error", "Disabled"].includes(x)) ||
            filterStates.includes(databaseState) ||
            databaseState === "Loading"
        ) &&
        (
            !filterStates.some((x) => ["Sharded", "NonSharded"].includes(x)) ||
            (filterStates.includes("Sharded") && db.sharded) ||
            (filterStates.includes("NonSharded") && !db.sharded)
        ) &&
        (
            !filterStates.some((x) => ["Local", "Remote"].includes(x)) ||
            (filterStates.includes("Local") && db.currentNode.relevant) ||
            (filterStates.includes("Remote") && !db.currentNode.relevant)
        )
    );
};

export const selectFilteredDatabaseNames = (store: RootState): string[] => {
    const criteria = selectDatabaseSearchCriteria(store);
    const allDatabases = selectAllDatabases(store);

    if (!(criteria.name || criteria.states?.length > 0)) {
        return allDatabases.map((x) => x.name);
    }

    let filteredDatabases = allDatabases;

    if (criteria.name) {
        filteredDatabases = filteredDatabases.filter((db) =>
            db.name.toLowerCase().includes(criteria.name.toLowerCase())
        );
    }

    if (criteria.states?.length > 0) {
        filteredDatabases = filteredDatabases.filter((db) => isDatabaseInFilterState(store, db, criteria.states));
    }

    return filteredDatabases.map((x) => x.name);
};

export const selectActiveDatabase = (store: RootState) => store.databases.activeDatabase;

export function selectDatabaseByName(name: string) {
    return (store: RootState) => {
        if (DatabaseUtils.isSharded(name)) {
            const rootDatabaseName = DatabaseUtils.shardGroupKey(name);
            const rootDatabase = databasesSelectors.selectById(
                store.databases.databases,
                rootDatabaseName
            ) as ShardedDatabaseSharedInfo;
            if (!rootDatabase) {
                return null;
            }
            return rootDatabase.shards.find((x) => x.name === name);
        }
        return databasesSelectors.selectById(store.databases.databases, name);
    };
}

function selectOrchestratorState(name: string) {
    return (store: RootState) => {
        const db = selectDatabaseByName(name)(store);

        return db.nodes.map((nodeInfo): locationAwareLoadableData<OrchestratorLocalInfo> => {
            const nodeTag = nodeInfo.tag;
            const loadState = localDatabaseDetailedLoadStatusSelectors.selectById(
                store.databases.localDetailedLoadStatus,
                nodeTag
            ) || {
                status: "idle",
                nodeTag,
            };

            switch (loadState.status) {
                case "idle":
                case "loading":
                case "failure":
                    return {
                        status: loadState.status,
                        location: {
                            nodeTag,
                        },
                        data: null,
                    };
                case "success": {
                    const data = localOrchestratorDetailedInfoSelectors.selectById(
                        store.databases.localOrchestratorDetailedInfo,
                        selectOrchestratorInfoId(name, nodeTag)
                    );

                    return {
                        location: {
                            nodeTag,
                        },
                        status: data ? "success" : "idle",
                        data,
                    };
                }
            }
        });
    };
}

export function selectDatabaseState(name: string) {
    return (store: RootState) => {
        const db = selectDatabaseByName(name)(store);

        const locations = DatabaseUtils.getLocations(db);

        return locations.map((location): locationAwareLoadableData<DatabaseLocalInfo> => {
            const loadState = localDatabaseDetailedLoadStatusSelectors.selectById(
                store.databases.localDetailedLoadStatus,
                location.nodeTag
            ) || {
                status: "idle",
                nodeTag: location.nodeTag,
            };

            switch (loadState.status) {
                case "idle":
                case "loading":
                case "failure":
                    return {
                        status: loadState.status,
                        location,
                    };
                case "success": {
                    const data = localDatabaseDetailedInfoSelectors.selectById(
                        store.databases.localDatabaseDetailedInfo,
                        selectDatabaseInfoId(name, location)
                    );

                    return {
                        location,
                        status: data ? "success" : "idle",
                        data,
                    };
                }
            }
        });
    };
}

/*
  For sharded databases it returns orchestrators state
  For non-sharded it return state from all nodes
 */
export function selectTopLevelState(name: string) {
    return (store: RootState): locationAwareLoadableData<TopLevelDatabaseInfo>[] => {
        const db = selectDatabaseByName(name)(store);

        if (db.sharded) {
            const state = selectOrchestratorState(db.name)(store);

            return state.map((orchestratorState) => {
                const { data, location, ...rest } = orchestratorState;

                return {
                    data: data ? localOrchestratorInfoIntoTopLevelDatabaseInfo(data, location.nodeTag) : null,
                    location,
                    ...rest,
                };
            });
        }

        const state = selectDatabaseState(db.name)(store);

        return state.map((dbState) => {
            const { data, location, ...rest } = dbState;

            return {
                data: data ? localDatabaseInfoIntoTopLevelDatabaseInfo(data, location.nodeTag) : null,
                location,
                ...rest,
            };
        });
    };
}

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
        setSearchCriteriaName: (state, action: PayloadAction<string>) => {
            state.searchCriteria.name = action.payload;
        },
        setSearchCriteriaStates: (state, action: PayloadAction<DatabaseFilterByStateOption[]>) => {
            state.searchCriteria.states = action.payload;
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

export function localDatabaseInfoIntoTopLevelDatabaseInfo(
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

export const { activeDatabaseChanged, databasesLoaded, initDetails, setSearchCriteriaName, setSearchCriteriaStates } =
    databasesSlice.actions;

export const loadDatabasesDetails = (nodeTags: string[]) => async (dispatch: AppDispatch) => {
    dispatch(initDetails(nodeTags));

    const tasks = nodeTags.map((nodeTag) => dispatch(fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const reloadDatabaseDetails =
    (databaseName: string): AppAsyncThunk =>
    async (dispatch: AppDispatch, getState) => {
        const nodeTags = selectClusterNodeTags(getState());
        const tasks = nodeTags.map((nodeTag) => dispatch(fetchDatabase({ nodeTag, databaseName })));
        await Promise.all(tasks);
    };

export const reloadDatabasesDetails: AppAsyncThunk = async (dispatch: AppDispatch, getState) => {
    const state = getState();
    const nodeTags = selectClusterNodeTags(state);

    const tasks = nodeTags.map((nodeTag) => dispatch(fetchDatabases(nodeTag)));

    await Promise.all(tasks);
};

export const throttledReloadDatabaseDetails = _.throttle(reloadDatabasesDetails, 100);

const fetchDatabases = createAsyncThunk(sliceName + "/fetchDatabases", async (nodeTag: string) => {
    return await services.databasesService.getDatabasesState(nodeTag);
});

const fetchDatabase = createAsyncThunk(
    sliceName + "/fetchDatabase",
    async (payload: { nodeTag: string; databaseName: string }) => {
        return await services.databasesService.getDatabaseState(payload.nodeTag, payload.databaseName);
    }
);

export const openCreateDatabaseDialog = () => () => {
    const createDbView = new createDatabase("newDatabase");
    app.showBootstrapDialog(createDbView);
};

export const openCreateDatabaseFromRestoreDialog = () => () => {
    const createDbView = new createDatabase("restore");
    app.showBootstrapDialog(createDbView);
};

export const confirmToggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk<{ can: boolean }> =>
    async () => {
        const confirmDeleteViewModel = new disableIndexingToggleConfirm(disable);
        app.showBootstrapDialog(confirmDeleteViewModel);
        return confirmDeleteViewModel.result;
    };

export const toggleIndexing =
    (db: DatabaseSharedInfo, disable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (disable) {
            await indexesService.disableAllIndexes(db);
            dispatch(databasesSlice.actions.disabledIndexing(db.name));
        } else {
            await indexesService.enableAllIndexes(db);
            dispatch(databasesSlice.actions.enabledIndexing(db.name));
        }
    };

export const openNotificationCenterForDatabase =
    (db: DatabaseSharedInfo): AppThunk =>
    (dispatch, getState) => {
        const activeDatabase = selectActiveDatabase(getState());
        if (activeDatabase !== db.name) {
            const dbRaw = databasesManager.default.getDatabaseByName(db.name);
            if (dbRaw) {
                databasesManager.default.activate(dbRaw);
            }
        }

        notificationCenter.instance.showNotifications.toggle();
    };

export const confirmTogglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean): AppAsyncThunk<{ can: boolean; locations: databaseLocationSpecifier[] }> =>
    async () => {
        //TODO: context selector!
        const msg = pause ? "pause indexing?" : "resume indexing?";
        const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to ` + msg);

        return {
            can: result.can,
            locations: DatabaseUtils.getLocations(db),
        };
    };

export const togglePauseIndexing =
    (db: DatabaseSharedInfo, pause: boolean, locations: databaseLocationSpecifier[]): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { indexesService } = getServices();

        if (pause) {
            const tasks = locations.map(async (l) => {
                await indexesService.pauseAllIndexes(db, l);
                dispatch(databasesSlice.actions.pausedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        } else {
            const tasks = locations.map(async (l) => {
                await indexesService.resumeAllIndexes(db, l);
                dispatch(databasesSlice.actions.resumedIndexing(db.name, l));
            });
            await Promise.all(tasks);
        }
    };

//TODO: report success after database deletion? - what about other actions?
export const confirmDeleteDatabases =
    (
        toDelete: DatabaseSharedInfo[]
    ): AppAsyncThunk<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> =>
    async (): Promise<{ can: boolean; keepFiles?: boolean; databases?: DatabaseSharedInfo[] }> => {
        const selectedDatabasesWithoutLock = toDelete.filter((x) => x.lockMode === "Unlock");
        if (selectedDatabasesWithoutLock.length === 0) {
            return {
                can: false,
            };
        }

        const confirmDeleteViewModel = new deleteDatabaseConfirm(selectedDatabasesWithoutLock);
        app.showBootstrapDialog(confirmDeleteViewModel);
        const baseResult = await confirmDeleteViewModel.result;
        return {
            ...baseResult,
            databases: selectedDatabasesWithoutLock,
        };
    };

export const deleteDatabases =
    (toDelete: DatabaseSharedInfo[], keepFiles: boolean): AppAsyncThunk<updateDatabaseConfigurationsResult> =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        /* TODO:
               const dbsList = toDelete.map(x => {
                   //TODO: x.isBeingDeleted(true);
                   const asDatabase = x.asDatabase();
    
                   // disconnect here to avoid race condition between database deleted message
                   // and websocket disconnection
                   //TODO: changesContext.default.disconnectIfCurrent(asDatabase, "DatabaseDeleted");
                   return asDatabase;
               });*/

        return databasesService.deleteDatabase(
            toDelete.map((x) => x.name),
            !keepFiles
        );
    };

export const compactDatabase = (database: DatabaseSharedInfo) => () => {
    const db = databasesManager.default.getDatabaseByName(database.name);
    if (db) {
        changesContext.default.disconnectIfCurrent(db, "DatabaseDisabled");
    }
    app.showBootstrapDialog(new compactDatabaseDialog(database));
};

export const changeDatabasesLockMode =
    (databases: DatabaseSharedInfo[], lockMode: DatabaseLockMode): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();

        await databasesService.setLockMode(databases, lockMode);
    };

export const confirmToggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk<boolean> =>
    async () => {
        const confirmation = new disableDatabaseToggleConfirm(databases, !enable);
        app.showBootstrapDialog(confirmation);

        const result = await confirmation.result;
        return result.can;
    };

export const toggleDatabases =
    (databases: DatabaseSharedInfo[], enable: boolean): AppAsyncThunk =>
    async (dispatch, getState, getServices) => {
        const { databasesService } = getServices();
        // TODO: lazy update UI
        await databasesService.toggle(databases, enable);
    };

/* TODO
    private onDatabaseDisabled(result: disableDatabaseResult) {
        const dbs = this.databases().sortedDatabases();
        const matchedDatabase = dbs.find(rs => rs.name === result.Name);

        if (matchedDatabase) {
            matchedDatabase.disabled(result.Disabled);

            // If Enabling a database (that is selected from the top) than we want it to be Online(Loaded)
            if (matchedDatabase.isCurrentlyActiveDatabase() && !matchedDatabase.disabled()) {
                new loadDatabaseCommand(matchedDatabase.asDatabase())
                    .execute();
            }
        }
    }
 */

export const confirmSetLockMode = (): AppAsyncThunk<boolean> => async () => {
    const result = await viewHelpers.confirmationMessage("Are you sure?", `Do you want to change lock mode?`);

    return result.can;
};

export const syncDatabaseDetails = (): AppThunk<UnsubscribeListener> => (dispatch) => {
    return dispatch(
        addAppListener({
            actionCreator: databasesSlice.actions.databasesLoaded,
            effect: (action, api) => {
                const state = api.getState();
                const existingData = state.databases.localDatabaseDetailedInfo.ids;

                const needsRefresh = action.payload.some((db) => {
                    const locations = DatabaseUtils.getLocations(db);
                    const ids = locations.map((l) => selectDatabaseInfoId(db.name, l));
                    return ids.some((id) => !existingData.includes(id));
                });

                if (needsRefresh) {
                    api.dispatch(throttledReloadDatabaseDetails);
                }
            },
        })
    );
};
