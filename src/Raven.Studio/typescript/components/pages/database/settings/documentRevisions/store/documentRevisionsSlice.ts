import { EntityState, PayloadAction, createAsyncThunk, createEntityAdapter, createSlice } from "@reduxjs/toolkit";
import { services } from "hooks/useServices";
import database from "models/resources/database";
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import { loadStatus } from "components/models/common";

export const documentRevisionsConfigNames = {
    defaultConflicts: "Conflicting Document Defaults",
    defaultDocument: "Document Defaults",
} as const;

export type DocumentRevisionsConfigName =
    | (typeof documentRevisionsConfigNames)[keyof typeof documentRevisionsConfigNames]
    | (string & NonNullable<unknown>);

export interface DocumentRevisionsConfig extends RevisionsCollectionConfiguration {
    Name: DocumentRevisionsConfigName;
}

export interface DocumentRevisionsState {
    loadStatus: loadStatus;
    selectedConfigNames: DocumentRevisionsConfigName[];
    configs: EntityState<DocumentRevisionsConfig>;
    originalConfigs: EntityState<DocumentRevisionsConfig>;
}

const configsAdapter = createEntityAdapter<DocumentRevisionsConfig>({
    selectId: (config) => config.Name,
});

const configsSelectors = configsAdapter.getSelectors();

const initialState: DocumentRevisionsState = {
    loadStatus: "idle",
    selectedConfigNames: [],
    configs: configsAdapter.getInitialState(),
    originalConfigs: configsAdapter.getInitialState(),
};

export const documentRevisionsSlice = createSlice({
    name: "documentRevisions",
    initialState,
    reducers: {
        addConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.addOne(state.configs, payload);
        },
        editConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.updateOne(state.configs, {
                id: payload.Name,
                changes: { ...payload },
            });
        },
        deleteConfig: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            configsAdapter.removeOne(state.configs, name);
        },
        toggleConfigState: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            const disabled = configsSelectors.selectById(state.configs, name).Disabled;

            configsAdapter.updateOne(state.configs, {
                id: name,
                changes: {
                    Disabled: !disabled,
                },
            });
        },
        toggleAllSelectedConfigNames: (state) => {
            if (state.selectedConfigNames.length === 0) {
                state.selectedConfigNames = configsSelectors.selectIds(state.configs) as DocumentRevisionsConfigName[];
            } else {
                state.selectedConfigNames = [];
            }
        },
        toggleSelectedConfigName: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            if (state.selectedConfigNames.includes(name)) {
                state.selectedConfigNames = state.selectedConfigNames.filter((selectedName) => selectedName !== name);
            } else {
                state.selectedConfigNames.push(name);
            }
        },
        deleteSelectedConfigs: (state) => {
            configsAdapter.removeMany(
                state.configs,
                state.selectedConfigNames.filter((name) => name !== documentRevisionsConfigNames.defaultConflicts)
            );
            state.selectedConfigNames = [];
        },
        disableSelectedConfigs: (state) => {
            configsAdapter.updateMany(
                state.configs,
                state.selectedConfigNames.map((name) => ({
                    id: name,
                    changes: {
                        Disabled: true,
                    },
                }))
            );
        },
        enableSelectedConfigs: (state) => {
            configsAdapter.updateMany(
                state.configs,
                state.selectedConfigNames.map((name) => ({
                    id: name,
                    changes: {
                        Disabled: false,
                    },
                }))
            );
        },
        saveConfigs: (state) => {
            configsAdapter.setAll(state.originalConfigs, configsSelectors.selectAll(state.configs));
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchConfigs.fulfilled, (state, { payload }) => {
                if (payload.config) {
                    if (payload.config.Default) {
                        configsAdapter.addOne(state.originalConfigs, {
                            ...payload.config.Default,
                            Name: documentRevisionsConfigNames.defaultDocument,
                        });
                    }

                    configsAdapter.addMany(
                        state.originalConfigs,
                        Object.keys(payload.config.Collections).map((name) => ({
                            ...payload.config.Collections[name],
                            Name: name,
                        }))
                    );
                }

                if (payload.conflictsConfig) {
                    configsAdapter.addOne(state.originalConfigs, {
                        ...payload.conflictsConfig,
                        Name: documentRevisionsConfigNames.defaultConflicts,
                    });
                } else {
                    configsAdapter.addOne(state.originalConfigs, {
                        Name: documentRevisionsConfigNames.defaultConflicts,
                        Disabled: false,
                        PurgeOnDelete: false,
                        MaximumRevisionsToDeleteUponDocumentUpdate: null,
                        MinimumRevisionAgeToKeep: "45.00:00:00",
                        MinimumRevisionsToKeep: null,
                    });
                }

                configsAdapter.setAll(state.configs, configsSelectors.selectAll(state.originalConfigs));
                state.loadStatus = "success";
            })
            .addCase(fetchConfigs.pending, (state) => {
                state.loadStatus = "loading";
            })
            .addCase(fetchConfigs.rejected, (state) => {
                state.loadStatus = "failure";
            });
    },
});

const fetchConfigs = createAsyncThunk<
    {
        config: RevisionsConfiguration;
        conflictsConfig: RevisionsCollectionConfiguration;
    },
    database
>(documentRevisionsSlice.name + "/fetchConfigs", async (db: database) => {
    const config = await services.databasesService.getRevisionsConfiguration(db);
    const conflictsConfig = await services.databasesService.getRevisionsForConflictsConfiguration(db);

    return {
        config,
        conflictsConfig,
    };
});

export const documentRevisionsActions = {
    ...documentRevisionsSlice.actions,
    fetchConfigs,
};

export const documentRevisionsSliceInternal = {
    configsSelectors,
};
