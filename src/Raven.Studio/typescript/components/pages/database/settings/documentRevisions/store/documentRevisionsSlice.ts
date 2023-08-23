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
    LoadStatus: loadStatus;
    Configs: EntityState<DocumentRevisionsConfig>;
    OriginalConfigs: EntityState<DocumentRevisionsConfig>;
}

const configsAdapter = createEntityAdapter<DocumentRevisionsConfig>({
    selectId: (config) => config.Name,
});

const configsSelectors = configsAdapter.getSelectors();

const initialState: DocumentRevisionsState = {
    LoadStatus: "idle",
    Configs: configsAdapter.getInitialState(),
    OriginalConfigs: configsAdapter.getInitialState(),
};

const sliceName = "documentRevisions";

export const documentRevisionsSlice = createSlice({
    name: sliceName,
    initialState,
    reducers: {
        addConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.addOne(state.Configs, payload);
        },
        editConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.updateOne(state.Configs, {
                id: payload.Name,
                changes: { ...payload },
            });
        },
        deleteConfig: (state, { payload }: PayloadAction<{ name: DocumentRevisionsConfigName }>) => {
            configsAdapter.removeOne(state.Configs, payload.name);
        },
        toggleConfigState: (state, { payload }: PayloadAction<{ name: DocumentRevisionsConfigName }>) => {
            const disabled = configsSelectors.selectById(state.Configs, payload.name).Disabled;

            configsAdapter.updateOne(state.Configs, {
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
                        configsAdapter.addOne(state.OriginalConfigs, {
                            ...payload.config.Default,
                            Name: documentRevisionsConfigNames.defaultDocument,
                        });
                    }

                    configsAdapter.addMany(
                        state.OriginalConfigs,
                        Object.keys(payload.config.Collections).map((name) => ({
                            ...payload.config.Collections[name],
                            Name: name,
                        }))
                    );
                }

                if (payload.conflictsConfig) {
                    configsAdapter.addOne(state.OriginalConfigs, {
                        ...payload.conflictsConfig,
                        Name: documentRevisionsConfigNames.defaultConflicts,
                    });
                } else {
                    configsAdapter.addOne(state.OriginalConfigs, {
                        Name: documentRevisionsConfigNames.defaultConflicts,
                        Disabled: false,
                        PurgeOnDelete: false,
                        MaximumRevisionsToDeleteUponDocumentUpdate: null,
                        MinimumRevisionAgeToKeep: "45.00:00:00",
                        MinimumRevisionsToKeep: null,
                    });
                }

                configsAdapter.setAll(state.Configs, configsSelectors.selectAll(state.OriginalConfigs));
                state.LoadStatus = "success";
            })
            .addCase(fetchConfigs.pending, (state) => {
                state.LoadStatus = "loading";
            })
            .addCase(fetchConfigs.rejected, (state) => {
                state.LoadStatus = "failure";
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
    loadStatus: (store: RootState) => store.documentRevisions.LoadStatus,
    defaultDocumentsConfig: (store: RootState) =>
        configsSelectors.selectById(store.documentRevisions.Configs, documentRevisionsConfigNames.defaultDocument),
    defaultConflictsConfig: (store: RootState) =>
        configsSelectors.selectById(store.documentRevisions.Configs, documentRevisionsConfigNames.defaultConflicts),
    collectionConfigs: (store: RootState) =>
        configsSelectors
            .selectAll(store.documentRevisions.Configs)
            .filter(
                (x) =>
                    x.Name !== documentRevisionsConfigNames.defaultConflicts &&
                    x.Name !== documentRevisionsConfigNames.defaultDocument
            ),
    collectionConfigsNames: (store: RootState) => configsSelectors.selectIds(store.documentRevisions.Configs),
    isAnyModified: (store: RootState) => {
        return !_.isEqual(store.documentRevisions.OriginalConfigs, store.documentRevisions.Configs);
    },
    originalConfigs: (store: RootState) => {
        return configsSelectors.selectAll(store.documentRevisions.OriginalConfigs);
    },
};
