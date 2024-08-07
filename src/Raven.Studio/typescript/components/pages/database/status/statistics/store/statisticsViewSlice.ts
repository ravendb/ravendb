import { createAsyncThunk, createEntityAdapter, createSelector, createSlice, EntityState } from "@reduxjs/toolkit";
import type { PayloadAction } from "@reduxjs/toolkit";
import EssentialDatabaseStatistics = Raven.Client.Documents.Operations.EssentialDatabaseStatistics;
import { AppDispatch, AppThunkApi, RootState } from "components/store";
import { services } from "hooks/useServices";
import DetailedDatabaseStatistics = Raven.Client.Documents.Operations.DetailedDatabaseStatistics;
import { loadableData, loadStatus, locationAwareLoadableData } from "components/models/common";
import IndexStats = Raven.Client.Documents.Indexes.IndexStats;
import {
    createFailureState,
    createIdleState,
    createSuccessState,
    databaseLocationComparator,
} from "components/utils/common";
import { IndexItem, PerLocationIndexStats } from "components/pages/database/status/statistics/store/models";
import { Draft } from "immer";
import { DatabaseSharedInfo } from "components/models/databases";
import DatabaseUtils from "components/utils/DatabaseUtils";

interface StatisticsState {
    databaseName: string;
    essentialStats: loadableData<EssentialDatabaseStatistics>;
    databaseDetails: EntityState<locationAwareLoadableData<DetailedDatabaseStatistics>, string>;
    indexDetailsLoadStatus: Array<{ location: databaseLocationSpecifier; status: loadStatus }>;
    indexDetails: EntityState<IndexItem, string>;
    ui: {
        detailsVisible: boolean;
        refreshing: boolean;
    };
}

function selectId(location: databaseLocationSpecifier) {
    return location.nodeTag + "__" + (location.shardNumber ?? "n-a");
}

const databaseStatsAdapter = createEntityAdapter<locationAwareLoadableData<DetailedDatabaseStatistics>, string>({
    selectId: (x) => selectId(x.location),
});

const indexStatsAdapter = createEntityAdapter<IndexItem, string>({
    selectId: (x) => x.sharedInfo.name,
    sortComparer: (a, b) => (a.sharedInfo.name > b.sharedInfo.name ? 1 : -1),
});

const indexSelectors = indexStatsAdapter.getSelectors();

const initialState: StatisticsState = {
    databaseName: null,
    databaseDetails: databaseStatsAdapter.getInitialState(),
    ui: {
        detailsVisible: false,
        refreshing: false,
    },
    indexDetailsLoadStatus: [],
    indexDetails: indexStatsAdapter.getInitialState(),
    essentialStats: createIdleState(),
};

const sliceName = "statistics";

const databaseNameSelector = (state: RootState) => state.statistics.databaseName;

const databaseStatsSelectors = databaseStatsAdapter.getSelectors<RootState>(
    (state) => state.statistics.databaseDetails
);
const selectAllDatabaseDetails = databaseStatsSelectors.selectAll;

const selectAllIndexesLoadStatus = (state: RootState) => state.statistics.indexDetailsLoadStatus;

const fetchEssentialStats = createAsyncThunk(sliceName + "/fetchEssentialStats", async (_, thunkAPI: AppThunkApi) => {
    const state = thunkAPI.getState();
    const dbName = databaseNameSelector(state);
    return services.databasesService.getEssentialStats(dbName);
});

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

const fetchDetailedDatabaseStats = createAsyncThunk(
    sliceName + "/fetchDetailedDatabaseStats",
    async (payload: {
        databaseName: string;
        location: databaseLocationSpecifier;
    }): Promise<DetailedDatabaseStatistics> => {
        return await services.databasesService.getDetailedStats(payload.databaseName, payload.location);
    }
);

const fetchDetailedIndexStats = createAsyncThunk(
    sliceName + "/fetchDetailedIndexStats",
    async (payload: { databaseName: string; location: databaseLocationSpecifier }): Promise<IndexStats[]> => {
        return await services.indexesService.getStats(payload.databaseName, payload.location);
    }
);

const fetchAllDetailedDatabaseStats = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();
    const locations = databaseStatsSelectors.selectAll(state).map((x) => x.location);

    const tasks = locations.map((location) =>
        dispatch(fetchDetailedDatabaseStats({ databaseName: state.statistics.databaseName, location })).unwrap()
    );
    await Promise.all(tasks);
};

const fetchAllDetailedIndexStats = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();

    const locations = databaseStatsSelectors.selectAll(state).map((x) => x.location);

    const tasks = locations.map((location) =>
        dispatch(fetchDetailedIndexStats({ databaseName: state.statistics.databaseName, location })).unwrap()
    );
    await Promise.all(tasks);
};

export const statisticsViewSlice = createSlice({
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
            const loadStateIdx = state.indexDetailsLoadStatus.findIndex((x) =>
                databaseLocationComparator(x.location, action.meta.arg.location)
            );
            state.indexDetailsLoadStatus[loadStateIdx].status = "success";

            const incomingNames = new Set(action.payload.map((x) => x.Name));
            const toDelete = state.indexDetails.ids.filter((x: string) => !incomingNames.has(x));
            indexStatsAdapter.removeMany(state.indexDetails, toDelete);

            action.payload.forEach((indexStat) => {
                const existingIndex = state.indexDetails.entities[indexStat.Name];

                let indexToUse: Draft<IndexItem>;

                if (!existingIndex) {
                    // create entry for new index
                    const locationsCount = state.indexDetailsLoadStatus.length;
                    indexToUse = {
                        sharedInfo: {
                            type: indexStat.Type,
                            name: indexStat.Name,
                            isReduceIndex:
                                indexStat.Type === "AutoMapReduce" ||
                                indexStat.Type === "MapReduce" ||
                                indexStat.Type === "JavaScriptMapReduce",
                            referencedCollections: indexStat.ReferencedCollections,
                        },
                        details: new Array(locationsCount).fill(null),
                    };
                    indexStatsAdapter.setOne(state.indexDetails, indexToUse);
                } else {
                    indexToUse = existingIndex;
                }
                const targetToSet = (indexToUse.details[loadStateIdx] ??= {} as PerLocationIndexStats);

                targetToSet.mapAttempts = indexStat.MapAttempts;
                targetToSet.mapErrors = indexStat.MapErrors;
                targetToSet.mapSuccesses = indexStat.MapSuccesses;
                targetToSet.isFaultyIndex = indexStat.Type === "Faulty";
                targetToSet.errorsCount = indexStat.ErrorsCount;
                targetToSet.entriesCount = indexStat.EntriesCount;
                targetToSet.mapReferenceAttempts = indexStat.MapReferenceAttempts;
                targetToSet.mapReferenceSuccesses = indexStat.MapReferenceSuccesses;
                targetToSet.mapReferenceErrors = indexStat.MapReferenceErrors;
                targetToSet.reduceAttempts = indexStat.ReduceAttempts;
                targetToSet.reduceSuccesses = indexStat.ReduceSuccesses;
                targetToSet.reduceErrors = indexStat.ReduceErrors;
                targetToSet.mappedPerSecondRate = indexStat.MappedPerSecondRate;
                targetToSet.reducedPerSecondRate = indexStat.ReducedPerSecondRate;
                targetToSet.isStale = indexStat.IsStale;
            });
        });

        builder.addCase(fetchDetailedIndexStats.pending, (state, action) => {
            const loadState = state.indexDetailsLoadStatus.find((x) =>
                databaseLocationComparator(x.location, action.meta.arg.location)
            );
            if (loadState.status !== "success") {
                loadState.status = "loading";
            }
        });
        builder.addCase(fetchDetailedIndexStats.rejected, (state, action) => {
            const loadState = state.indexDetailsLoadStatus.find((x) =>
                databaseLocationComparator(x.location, action.meta.arg.location)
            );

            loadState.status = "failure";
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
                state.ui.detailsVisible = false;

                const payloadLocationIds = action.payload.locations.map((x) => selectId(x));
                const isForCurrentLocations = _.isEqual(state.databaseDetails.ids, payloadLocationIds);

                const isForCurrentDatabase = state.databaseName === action.payload.databaseName;

                if (isForCurrentDatabase && isForCurrentLocations) {
                    return;
                }

                state.essentialStats = initialState.essentialStats;
                state.databaseName = action.payload.databaseName;

                databaseStatsAdapter.setAll(
                    state.databaseDetails,
                    action.payload.locations.map((location) => ({
                        location,
                        ...createIdleState(),
                    }))
                );

                state.indexDetailsLoadStatus = action.payload.locations.map((location) => ({
                    location,
                    loadError: null,
                    status: "idle",
                }));

                state.indexDetails = indexStatsAdapter.getInitialState();
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
            state.ui.detailsVisible = true;
        },
        hideDetails: (state) => {
            state.ui.detailsVisible = false;
        },
        refreshStarted: (state) => {
            state.ui.refreshing = true;
        },
        refreshFinished: (state) => {
            state.ui.refreshing = false;
        },
    },
});

const { initForDatabase, refreshStarted, refreshFinished } = statisticsViewSlice.actions;

export const initView = (db: DatabaseSharedInfo) => async (dispatch: AppDispatch, getState: () => RootState) => {
    const firstTime =
        selectEssentialStats(getState()).status === "idle" || db.name !== getState().statistics.databaseName;

    if (firstTime) {
        dispatch(initForDatabase(db.name, DatabaseUtils.getLocations(db)));
        dispatch(fetchEssentialStats());
    } else {
        dispatch(refresh());
    }
};

export const toggleDetails = () => async (dispatch: AppDispatch, getState: () => RootState) => {
    const state = getState();
    const visible = state.statistics.ui.detailsVisible;

    if (visible) {
        dispatch(statisticsViewSlice.actions.hideDetails());
    } else {
        dispatch(statisticsViewSlice.actions.showDetails());

        if (needsDetailsRefresh(state)) {
            const dbStatsTask = dispatch(fetchAllDetailedDatabaseStats());
            const indexesStatsTask = dispatch(fetchAllDetailedIndexStats());

            await Promise.all([dbStatsTask, indexesStatsTask]);
        }
    }
};

const needsDetailsRefresh = createSelector(databaseStatsSelectors.selectAll, (items) =>
    items.every((x) => x.status === "idle")
);

const selectEssentialStats = (state: RootState) => state.statistics.essentialStats;
const selectDetailsVisible = (state: RootState) => state.statistics.ui.detailsVisible;
const selectRefreshing = (state: RootState) => state.statistics.ui.refreshing;

const selectGlobalIndexDetailsStatus = (state: RootState): loadStatus => {
    const statuses = state.statistics.indexDetailsLoadStatus.map((x) => x.status);
    if (statuses.every((x) => x === "failure")) {
        return "failure";
    }

    if (statuses.some((x) => x === "success")) {
        return "success";
    }

    return "loading";
};

const selectIndexByName = (indexName: string) => (state: RootState) =>
    state.statistics.indexDetails.entities[indexName];
const selectMapIndexNames = (state: RootState) => {
    const indexes = indexSelectors.selectAll(state.statistics.indexDetails);
    return indexes.filter((x) => !x.sharedInfo.isReduceIndex).map((x) => x.sharedInfo.name);
};

const selectMapReduceIndexNames = (state: RootState) => {
    const indexes = indexSelectors.selectAll(state.statistics.indexDetails);
    return indexes.filter((x) => x.sharedInfo.isReduceIndex).map((x) => x.sharedInfo.name);
};

export const statisticsViewSelectors = {
    allDatabaseDetails: selectAllDatabaseDetails,
    allIndexesLoadStatus: selectAllIndexesLoadStatus,
    essentialStats: selectEssentialStats,
    detailsVisible: selectDetailsVisible,
    refreshing: selectRefreshing,
    globalIndexDetailsStatus: selectGlobalIndexDetailsStatus,
    indexByName: selectIndexByName,
    mapIndexNames: selectMapIndexNames,
    mapReduceIndexNames: selectMapReduceIndexNames,
};
