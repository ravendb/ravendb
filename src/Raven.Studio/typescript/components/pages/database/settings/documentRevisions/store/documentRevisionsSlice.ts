import { EntityState, PayloadAction, createAsyncThunk, createEntityAdapter, createSlice } from "@reduxjs/toolkit";
import { services } from "hooks/useServices";
import database from "models/resources/database";
import RevisionsConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsConfiguration;
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import { RootState } from "components/store";

export type DocumentRevisionsConfigName =
    | "Conflicting Document Defaults"
    | "Document Defaults"
    | (string & NonNullable<unknown>);

export interface DocumentRevisionsConfig extends RevisionsCollectionConfiguration {
    Name: DocumentRevisionsConfigName;
}

export interface DocumentRevisionsState {
    FetchStatus: "success" | "loading" | "error";
    Configs: EntityState<DocumentRevisionsConfig>;
    OriginalConfigs: EntityState<DocumentRevisionsConfig>;
}

const configsAdapter = createEntityAdapter<DocumentRevisionsConfig>({
    selectId: (config) => config.Name,
});

const configsSelectors = configsAdapter.getSelectors();

const initialState: DocumentRevisionsState = {
    FetchStatus: "loading",
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
                            Name: "Document Defaults",
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
                        Name: "Conflicting Document Defaults",
                    });
                } else {
                    configsAdapter.addOne(state.OriginalConfigs, {
                        Name: "Conflicting Document Defaults",
                        Disabled: false,
                        PurgeOnDelete: false,
                        MaximumRevisionsToDeleteUponDocumentUpdate: null,
                        MinimumRevisionAgeToKeep: "45.00:00:00",
                        MinimumRevisionsToKeep: null,
                    });
                }

                configsAdapter.setAll(state.Configs, configsSelectors.selectAll(state.OriginalConfigs));
                state.FetchStatus = "success";
            })
            .addCase(fetchConfigs.pending, (state) => {
                state.FetchStatus = "loading";
            })
            .addCase(fetchConfigs.rejected, (state) => {
                state.FetchStatus = "error";
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
    fetchStatus: (store: RootState) => store.documentRevisions.FetchStatus,
    defaultDocumentsConfig: (store: RootState) =>
        configsSelectors.selectById(
            store.documentRevisions.Configs,
            "Document Defaults" satisfies DocumentRevisionsConfigName
        ),
    defaultConflictsConfig: (store: RootState) =>
        configsSelectors.selectById(
            store.documentRevisions.Configs,
            "Conflicting Document Defaults" satisfies DocumentRevisionsConfigName
        ),
    collectionConfigs: (store: RootState) =>
        configsSelectors
            .selectAll(store.documentRevisions.Configs)
            .filter((x) => x.Name !== "Conflicting Document Defaults" && x.Name !== "Document Defaults"),
    isAnyModified: (store: RootState) => {
        return _.isEqual(store.documentRevisions.OriginalConfigs, store.documentRevisions.Configs);
    },
    originalConfigs: (store: RootState) => {
        return configsSelectors.selectAll(store.documentRevisions.OriginalConfigs);
    },
};
