import { EntityState, PayloadAction, createAsyncThunk, createEntityAdapter, createSlice } from "@reduxjs/toolkit";
import { services } from "hooks/useServices";
import RevisionsCollectionConfiguration = Raven.Client.Documents.Operations.Revisions.RevisionsCollectionConfiguration;
import { loadStatus } from "components/models/common";

export const documentRevisionsConfigNames = {
    defaultConflicts: "Conflicting Document Defaults",
    defaultDocument: "Document Defaults",
    conversations: "@conversations",
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
    configs: EntityState<DocumentRevisionsConfig, string>;
    originalConfigs: EntityState<DocumentRevisionsConfig, string>;
}

const configsAdapter = createEntityAdapter<DocumentRevisionsConfig, string>({
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
        configAdded: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.addOne(state.configs, payload);
        },
        configEdited: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            configsAdapter.updateOne(state.configs, {
                id: payload.Name,
                changes: { ...payload },
            });
        },
        configDeleted: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            configsAdapter.removeOne(state.configs, name);
        },
        configStateToggled: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            const disabled = configsSelectors.selectById(state.configs, name).Disabled;

            configsAdapter.updateOne(state.configs, {
                id: name,
                changes: {
                    Disabled: !disabled,
                },
            });
        },
        allSelectedConfigNamesToggled: (state) => {
            if (state.selectedConfigNames.length === 0) {
                state.selectedConfigNames = (
                    configsSelectors.selectIds(state.configs) as DocumentRevisionsConfigName[]
                ).filter((name) => name !== documentRevisionsConfigNames.conversations);
            } else {
                state.selectedConfigNames = [];
            }
        },
        selectedConfigNameToggled: (state, { payload: name }: PayloadAction<DocumentRevisionsConfigName>) => {
            if (state.selectedConfigNames.includes(name)) {
                state.selectedConfigNames = state.selectedConfigNames.filter((selectedName) => selectedName !== name);
            } else {
                state.selectedConfigNames.push(name);
            }
        },
        selectedConfigsDeleted: (state) => {
            configsAdapter.removeMany(
                state.configs,
                state.selectedConfigNames.filter(
                    (name) =>
                        name !== documentRevisionsConfigNames.defaultConflicts &&
                        name !== documentRevisionsConfigNames.conversations
                )
            );
            state.selectedConfigNames = [];
        },
        selectedConfigsDisabled: (state) => {
            configsAdapter.updateMany(
                state.configs,
                state.selectedConfigNames
                    .filter((name) => name !== documentRevisionsConfigNames.conversations)
                    .map((name) => ({
                        id: name,
                        changes: {
                            Disabled: true,
                        },
                    }))
            );
        },
        selectedConfigsEnabled: (state) => {
            configsAdapter.updateMany(
                state.configs,
                state.selectedConfigNames
                    .filter((name) => name !== documentRevisionsConfigNames.conversations)
                    .map((name) => ({
                        id: name,
                        changes: {
                            Disabled: false,
                        },
                    }))
            );
        },
        configsSaved: (state) => {
            configsAdapter.setAll(state.originalConfigs, configsSelectors.selectAll(state.configs));
        },
        reset: () => initialState,
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

const fetchConfigs = createAsyncThunk(documentRevisionsSlice.name + "/fetchConfigs", async (databaseName: string) => {
    const config = await services.databasesService.getRevisionsConfiguration(databaseName);
    const conflictsConfig = await services.databasesService.getRevisionsForConflictsConfiguration(databaseName);

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
