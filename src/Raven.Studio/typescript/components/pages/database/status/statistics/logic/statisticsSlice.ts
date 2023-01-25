import { createAsyncThunk, createEntityAdapter, createSelector, createSlice, EntityState } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { AppDispatch, AppThunkApi, RootState } from "components/store";
import { services } from "hooks/useServices";
import databasesManager from "common/shell/databasesManager";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import { loadableData, locationAwareLoadableData } from "components/models/common";
import database from "models/resources/database";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";
import { DetailedIndexStats, PerIndexStats } from "components/pages/database/status/statistics/logic/models";

export interface StatisticsState {
    databaseName: string;
    databaseDetails: EntityState<locationAwareLoadableData<DetailedDatabaseStatistics>>;
    indexDetails: EntityState<locationAwareLoadableData<IndexStats[]>>;
    detailsVisible: boolean;
    essentialStats: loadableData<EssentialDatabaseStatistics>;
    refreshing: boolean;
}

function selectId(location: databaseLocationSpecifier) {
    return location.nodeTag + "__" + (location.shardNumber ?? "n-a");
}

const databaseStatsAdapter = createEntityAdapter<locationAwareLoadableData<DetailedDatabaseStatistics>>({
    selectId: (x) => selectId(x.location),
});

const indexStatsAdapter = createEntityAdapter<locationAwareLoadableData<IndexStats[]>>({
    selectId: (x) => selectId(x.location),
});

const initialState: StatisticsState = {
    databaseName: null,
    detailsVisible: false,
    databaseDetails: databaseStatsAdapter.getInitialState(),
    indexDetails: indexStatsAdapter.getInitialState(),
    essentialStats: createIdleState(),
    refreshing: false,
};

const sliceName = "statistics";

const databaseNameSelector = (state: RootState) => state.statistics.databaseName;

const databaseSelectors = databaseStatsAdapter.getSelectors<RootState>((state) => state.statistics.databaseDetails);
export const selectAllDatabaseDetails = databaseSelectors.selectAll;

const indexesSelectors = indexStatsAdapter.getSelectors<RootState>((state) => state.statistics.indexDetails);

export const fetchEssentialStats = createAsyncThunk(
    sliceName + "/fetchEssentialStats",
    async (_, thunkAPI: AppThunkApi) => {
        const state = thunkAPI.getState();
        const dbName = databaseNameSelector(state);
        const db = databasesManager.default.getDatabaseByName(dbName);
        return services.databasesService.getEssentialStats(db);
    }
);

export const refresh = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    dispatch(refreshStarted());

    try {
        await dispatch(fetchEssentialStats());
        const detailsVisible = selectDetailsVisible(getState());

        if (detailsVisible) {
            const dbTask = dispatch(fetchAllDetailedDatabaseStats());
            const indexTask = dispatch(fetchAllDetailedIndexStats());
            await Promise.all([dbTask, indexTask]);
        }
    } finally {
        dispatch(refreshFinished());
    }
};

export const fetchDetailedDatabaseStats = createAsyncThunk(
    sliceName + "/fetchDetailedDatabaseStats",
    async (payload: { db: database; location: databaseLocationSpecifier }): Promise<DetailedDatabaseStatistics> => {
        return await services.databasesService.getDetailedStats(payload.db, payload.location);
    }
);

export const fetchDetailedIndexStats = createAsyncThunk(
    sliceName + "/fetchDetailedIndexStats",
    async (payload: { db: database; location: databaseLocationSpecifier }): Promise<IndexStats[]> => {
        return await services.indexesService.getStats(payload.db, payload.location);
    }
);

export const fetchAllDetailedDatabaseStats = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();
    const locations = databaseSelectors.selectAll(state).map((x) => x.location);

    const db = databasesManager.default.getDatabaseByName(state.statistics.databaseName);

    const tasks = locations.map((location) => dispatch(fetchDetailedDatabaseStats({ db, location })).unwrap());
    await Promise.all(tasks);
};

export const fetchAllDetailedIndexStats = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();

    const locations = databaseSelectors.selectAll(state).map((x) => x.location);

    const db = databasesManager.default.getDatabaseByName(state.statistics.databaseName);

    const tasks = locations.map((location) => dispatch(fetchDetailedIndexStats({ db, location })).unwrap());
    await Promise.all(tasks);
};

export const statisticsSlice = createSlice({
    extraReducers: (builder) => {
        builder.addCase(fetchEssentialStats.pending, (state) => {
            state.essentialStats.status = "loading";
        });
        builder.addCase(fetchEssentialStats.fulfilled, (state, action) => {
            state.essentialStats = createSuccessState(action.payload);
        });
        builder.addCase(fetchEssentialStats.rejected, (state) => {
            state.essentialStats = createFailureState();
        });

        builder.addCase(fetchDetailedIndexStats.fulfilled, (state, action) => {
            indexStatsAdapter.setOne(state.indexDetails, {
                ...createSuccessState(action.payload),
                location: action.meta.arg.location,
            });
        });
        builder.addCase(fetchDetailedIndexStats.pending, (state, action) => {
            const id = selectId(action.meta.arg.location);
            indexStatsAdapter.updateOne(state.indexDetails, {
                id,
                changes: {
                    status: "loading",
                    error: null,
                },
            });
        });
        builder.addCase(fetchDetailedIndexStats.rejected, (state, action) => {
            indexStatsAdapter.setOne(state.indexDetails, {
                ...createFailureState(),
                location: action.meta.arg.location,
            });
        });

        builder.addCase(fetchDetailedDatabaseStats.fulfilled, (state, action) => {
            databaseStatsAdapter.setOne(state.databaseDetails, {
                ...createSuccessState(action.payload),
                location: action.meta.arg.location,
            });
        });
        builder.addCase(fetchDetailedDatabaseStats.pending, (state, action) => {
            const id = selectId(action.meta.arg.location);
            databaseStatsAdapter.updateOne(state.databaseDetails, {
                id,
                changes: {
                    status: "loading",
                    error: null,
                },
            });
        });
        builder.addCase(fetchDetailedDatabaseStats.rejected, (state, action) => {
            databaseStatsAdapter.setOne(state.databaseDetails, {
                ...createFailureState(),
                location: action.meta.arg.location,
            });
        });
    },
    initialState,
    name: "statistics",
    reducers: {
        initForDatabase: {
            reducer: (
                state,
                action: PayloadAction<{ databaseName: string; locations: databaseLocationSpecifier[] }>
            ) => {
                state.detailsVisible = false;

                if (state.databaseName !== action.payload.databaseName) {
                    state.essentialStats = initialState.essentialStats;
                    state.databaseName = action.payload.databaseName;

                    databaseStatsAdapter.setAll(
                        state.databaseDetails,
                        action.payload.locations.map((location) => ({
                            location,
                            ...createIdleState(),
                        }))
                    );

                    indexStatsAdapter.setAll(
                        state.indexDetails,
                        action.payload.locations.map((location) => ({
                            location,
                            ...createIdleState(),
                        }))
                    );
                }
            },
            prepare: (databaseName: string, locations: databaseLocationSpecifier[]) => {
                return {
                    payload: {
                        databaseName,
                        locations,
                    },
                };
            },
        },
        showDetails: (state) => {
            state.detailsVisible = true;
        },
        hideDetails: (state) => {
            state.detailsVisible = false;
        },
        refreshStarted: (state) => {
            state.refreshing = true;
        },
        refreshFinished: (state) => {
            state.refreshing = false;
        },
    },
});

const { initForDatabase, refreshStarted, refreshFinished } = statisticsSlice.actions;

export const initView = (db: database) => async (dispatch: AppDispatch, getState: () => RootState) => {
    dispatch(initForDatabase(db.name, db.getLocations()));

    const firstTime = selectEssentialStats(getState()).status === "idle";

    if (firstTime) {
        dispatch(fetchEssentialStats());
    } else {
        dispatch(refresh());
    }
};

export const toggleDetails = async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();
    const visible = state.statistics.detailsVisible;

    if (visible) {
        dispatch(statisticsSlice.actions.hideDetails());
    } else {
        dispatch(statisticsSlice.actions.showDetails());

        if (needsDetailsRefresh(state)) {
            const dbStatsTask = dispatch(fetchAllDetailedDatabaseStats());
            const indexesStatsTask = dispatch(fetchAllDetailedIndexStats());

            await Promise.all([dbStatsTask, indexesStatsTask]);
        }
    }
};

const needsDetailsRefresh = createSelector(databaseSelectors.selectAll, (items) =>
    items.every((x) => x.status === "idle")
);

export const selectEssentialStats = (state: RootState) => state.statistics.essentialStats;
export const selectDetailsVisible = (state: RootState) => state.statistics.detailsVisible;

export const selectRefreshing = (state: RootState) => state.statistics.refreshing;

export const selectAllIndexDetails = indexesSelectors.selectAll;
export const selectIndexesGroup = createSelector(selectAllIndexDetails, mapIndexStats);

export function mapIndexStats(data: locationAwareLoadableData<IndexStats[]>[]): DetailedIndexStats {
    const allFailures = data.every((x) => x.status === "failure");
    if (allFailures) {
        return {
            noData: true,
            groups: [],
            globalState: "failure",
            perLocationStatus: [],
        };
    }

    const atLeastOneSuccess = data.find((x) => x.status === "success"); // it means we have indexes list, so we can show table

    if (!atLeastOneSuccess) {
        return {
            noData: false,
            globalState: "loading",
            groups: [],
            perLocationStatus: [],
        };
    }

    if (atLeastOneSuccess && atLeastOneSuccess.data.length === 0) {
        return {
            noData: true,
            groups: [],
            globalState: "success",
            perLocationStatus: [],
        };
    }

    // at this point we have al least one NON-EMPTY success -> we can group indexes

    const perLocationStatus = data.map((x) => ({
        location: x.location,
        status: x.status,
        data: null as never,
        error: x.error,
    }));

    const indexes: PerIndexStats[] = [];

    for (let i = 0; i < data.length; i++) {
        const datum = data[i];
        if (datum.status === "success") {
            for (const indexStats of datum.data) {
                const existingItem = indexes.find((x) => x.name === indexStats.Name);

                let itemToUpdate: PerIndexStats;

                if (existingItem) {
                    itemToUpdate = existingItem;
                } else {
                    itemToUpdate = {
                        name: indexStats.Name,
                        type: indexStats.Type,
                        details: [...Array(data.length)],
                        isReduceIndex:
                            indexStats.Type === "AutoMapReduce" ||
                            indexStats.Type === "MapReduce" ||
                            indexStats.Type === "JavaScriptMapReduce",
                    };
                    indexes.push(itemToUpdate);
                }

                itemToUpdate.details[i] = {
                    mapAttempts: indexStats.MapAttempts,
                    mapErrors: indexStats.MapErrors,
                    mapSuccesses: indexStats.MapSuccesses,
                    isFaultyIndex: indexStats.Type === "Faulty",
                    errorsCount: indexStats.ErrorsCount,
                    entriesCount: indexStats.EntriesCount,
                    mapReferenceAttempts: indexStats.MapReferenceAttempts,
                    mapReferenceSuccesses: indexStats.MapReferenceSuccesses,
                    mapReferenceErrors: indexStats.MapReferenceErrors,
                    reduceAttempts: indexStats.ReduceAttempts,
                    reduceSuccesses: indexStats.ReduceSuccesses,
                    reduceErrors: indexStats.ReduceErrors,
                    mappedPerSecondRate: indexStats.MappedPerSecondRate,
                    reducedPerSecondRate: indexStats.ReducedPerSecondRate,
                    isStale: indexStats.IsStale,
                };
            }
        }
    }

    indexes.sort((a, b) => (a.name > b.name ? 1 : -1));

    const types = Array.from(new Set(indexes.map((x) => x.type)));
    types.sort();

    const groups = types.map((type) => {
        return {
            type,
            indexes: indexes.filter((i) => i.type === type),
        };
    });
    return {
        perLocationStatus,
        groups,
        noData: false,
        globalState: "success",
    };
}
