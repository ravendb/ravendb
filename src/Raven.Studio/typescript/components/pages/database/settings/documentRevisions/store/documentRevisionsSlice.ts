import { EntityState, PayloadAction, createAsyncThunk, createEntityAdapter, createSlice } from "@reduxjs/toolkit";
import { services } from "hooks/useServices";
import database from "models/resources/database";
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import { RootState } from "components/store";
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
    configs: EntityState<DocumentRevisionsConfig>;
    originalConfigs: EntityState<DocumentRevisionsConfig>;
}

const configsAdapter = createEntityAdapter<DocumentRevisionsConfig>({
    selectId: (config) => config.Name,
});

const configsSelectors = configsAdapter.getSelectors();

const initialState: DocumentRevisionsState = {
    loadStatus: "idle",
    configs: configsAdapter.getInitialState(),
    originalConfigs: configsAdapter.getInitialState(),
};

const sliceName = "documentRevisions";

export const documentRevisionsSlice = createSlice({
    name: sliceName,
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
        deleteConfig: (state, { payload }: PayloadAction<{ name: DocumentRevisionsConfigName }>) => {
            configsAdapter.removeOne(state.configs, payload.name);
        },
        toggleConfigState: (state, { payload }: PayloadAction<{ name: DocumentRevisionsConfigName }>) => {
            const disabled = configsSelectors.selectById(state.configs, payload.name).Disabled;

            configsAdapter.updateOne(state.configs, {
                id: payload.name,
                changes: {
                    Disabled: !disabled,
                },
            });
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
>(sliceName + "/fetchConfigs", async (db: database) => {
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

export const documentRevisionsSelectors = {
    loadStatus: (store: RootState) => store.documentRevisions.loadStatus,
    defaultDocumentsConfig: (store: RootState) =>
        configsSelectors.selectById(store.documentRevisions.configs, documentRevisionsConfigNames.defaultDocument),
    defaultConflictsConfig: (store: RootState) =>
        configsSelectors.selectById(store.documentRevisions.configs, documentRevisionsConfigNames.defaultConflicts),
    collectionConfigs: (store: RootState) =>
        configsSelectors
            .selectAll(store.documentRevisions.configs)
            .filter(
                (x) =>
                    x.Name !== documentRevisionsConfigNames.defaultConflicts &&
                    x.Name !== documentRevisionsConfigNames.defaultDocument
            ),
    collectionConfigsNames: (store: RootState) => configsSelectors.selectIds(store.documentRevisions.configs),
    isAnyModified: (store: RootState) => {
        return !_.isEqual(store.documentRevisions.originalConfigs, store.documentRevisions.configs);
    },
    originalConfigs: (store: RootState) => {
        return configsSelectors.selectAll(store.documentRevisions.originalConfigs);
    },
};
