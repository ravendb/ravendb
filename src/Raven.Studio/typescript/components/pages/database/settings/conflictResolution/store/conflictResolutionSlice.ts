import {
    EntityState,
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    PayloadAction,
    createSelector,
} from "@reduxjs/toolkit";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import { RootState } from "components/store";
import database from "models/resources/database";

export interface ConflictResolutionCollectionConfig {
    id: string;
    name: string;
    script: string;
    lastModifiedTime: string;
    isInEditMode: boolean;
    isNewUnsaved: boolean;
    isEdited: boolean;
}

interface ConflictResolutionConfig {
    isResolveToLatest: boolean;
    collectionConfigs: EntityState<ConflictResolutionCollectionConfig, string>;
}

interface ConflictResolutionState {
    loadStatus: loadStatus;
    config: ConflictResolutionConfig;
    isDirty: boolean;
}

const collectionConfigsAdapter = createEntityAdapter<ConflictResolutionCollectionConfig, string>({
    selectId: (collection) => collection.id,
    sortComparer: (a, b) => (a.lastModifiedTime ?? "").localeCompare(b.lastModifiedTime ?? ""),
});

const collectionConfigsSelectors = collectionConfigsAdapter.getSelectors();

const initialState: ConflictResolutionState = {
    loadStatus: "idle",
    isDirty: false,
    config: {
        isResolveToLatest: false,
        collectionConfigs: collectionConfigsAdapter.getInitialState(),
    },
};

const setAll = (state: ConflictResolutionState, payload: Raven.Client.ServerWide.ConflictSolver) => {
    state.config.isResolveToLatest = payload.ResolveToLatest;

    collectionConfigsAdapter.setAll(
        state.config.collectionConfigs,
        Object.keys(payload.ResolveByCollection).map((collectionName) => ({
            id: _.uniqueId(),
            name: collectionName,
            script: payload.ResolveByCollection[collectionName].Script,
            lastModifiedTime: payload.ResolveByCollection[collectionName].LastModifiedTime,
            isNewUnsaved: false,
            isInEditMode: false,
            isEdited: false,
        }))
    );
};

export const conflictResolutionSlice = createSlice({
    name: "conflictResolution",
    initialState,
    reducers: {
        toggleIsResolveToLatest: (state) => {
            state.config.isResolveToLatest = !state.config.isResolveToLatest;
            state.isDirty = true;
        },
        add: (state) => {
            collectionConfigsAdapter.addOne(state.config.collectionConfigs, {
                id: _.uniqueId(),
                name: "",
                script: "",
                lastModifiedTime: null,
                isNewUnsaved: true,
                isInEditMode: true,
                isEdited: false,
            });
        },
        edit: (state, { payload: id }: { payload: string }) => {
            collectionConfigsAdapter.updateOne(state.config.collectionConfigs, {
                id,
                changes: {
                    isInEditMode: true,
                },
            });
        },
        discardEdit: (state, { payload: id }: { payload: string }) => {
            const collection = collectionConfigsSelectors.selectById(state.config.collectionConfigs, id);

            if (collection.isNewUnsaved) {
                collectionConfigsAdapter.removeOne(state.config.collectionConfigs, id);
            } else {
                collectionConfigsAdapter.updateOne(state.config.collectionConfigs, {
                    id,
                    changes: {
                        isInEditMode: false,
                    },
                });
            }
        },
        saveEdit: (
            state,
            {
                payload,
            }: {
                payload: {
                    id: string;
                    newConfig: Pick<ConflictResolutionCollectionConfig, "name" | "script">;
                };
            }
        ) => {
            collectionConfigsAdapter.updateOne(state.config.collectionConfigs, {
                id: payload.id,
                changes: {
                    ...payload.newConfig,
                    isInEditMode: false,
                    isEdited: true,
                    isNewUnsaved: false,
                },
            });
            state.isDirty = true;
        },
        allSaved: (state, action: PayloadAction<Raven.Client.ServerWide.ConflictSolver>) => {
            setAll(state, action.payload);
            state.isDirty = false;
        },
        delete: (state, { payload: id }: { payload: string }) => {
            collectionConfigsAdapter.removeOne(state.config.collectionConfigs, id);
            state.isDirty = true;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchConfig.fulfilled, (state, { payload }) => {
                if (!payload) {
                    state.loadStatus = "success";
                    return;
                }

                setAll(state, payload);

                state.loadStatus = "success";
            })
            .addCase(fetchConfig.pending, (state) => {
                state.loadStatus = "loading";
            })
            .addCase(fetchConfig.rejected, (state) => {
                state.loadStatus = "failure";
            });
    },
});

const fetchConfig = createAsyncThunk<Raven.Client.ServerWide.ConflictSolver, string>(
    "conflictResolution/fetchConfig",
    async (db) => {
        return await services.databasesService.getConflictSolverConfiguration(db);
    }
);

export const conflictResolutionActions = {
    ...conflictResolutionSlice.actions,
    fetchConfig,
};

const collectionConfigs = (store: RootState) =>
    collectionConfigsSelectors.selectAll(store.conflictResolution.config.collectionConfigs);

export const conflictResolutionSelectors = {
    loadStatus: (store: RootState) => store.conflictResolution.loadStatus,
    isResolveToLatest: (store: RootState) => store.conflictResolution.config.isResolveToLatest,
    collectionConfigs,
    usedCollectionNames: createSelector(collectionConfigs, (configs) => configs.map((x) => x.name)),
    isDirty: (store: RootState) => store.conflictResolution.isDirty,
    isSomeInEditMode: (store: RootState) =>
        collectionConfigsSelectors
            .selectAll(store.conflictResolution.config.collectionConfigs)
            .some((x) => x.isInEditMode),
};
