import { PayloadAction, createAsyncThunk, createSlice } from "@reduxjs/toolkit";
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
    Config: {
        Collections: DocumentRevisionsConfig[];
        Default: DocumentRevisionsConfig;
    };
    ConflictsConfig: DocumentRevisionsConfig;
}

const initialState: DocumentRevisionsState = {
    FetchStatus: "loading",
    Config: {
        Collections: [],
        Default: null,
    },
    ConflictsConfig: {
        Name: "Conflicting Document Defaults",
        Disabled: false,
        PurgeOnDelete: false,
        MinimumRevisionsToKeep: undefined,
        MinimumRevisionAgeToKeep: "45.00:00:00",
    },
};

const sliceName = "documentRevisions";

export const documentRevisionsSlice = createSlice({
    name: sliceName,
    initialState,
    reducers: {
        addDocumentDefaultsConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            state.Config.Default = payload;
        },
        editDocumentDefaultsConfig: (...args) => {
            documentRevisionsSlice.actions.editDocumentDefaultsConfig(args);
        },
        toggleDocumentDefaultsConfig: (state) => {
            state.Config.Default.Disabled = !state.Config.Default.Disabled;
        },
        deleteDocumentDefaultsConfig: (state) => {
            state.Config.Default = null;
        },
        addCollectionConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            state.Config.Collections.push(payload);
        },
        editCollectionConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            state.Config.Collections.push(payload);
            const idx = state.Config.Collections.indexOf(state.Config.Collections.find((x) => x.Name === payload.Name));
            state.Config.Collections[idx] = payload;
        },
        toggleCollectionConfig: (state, { payload }: PayloadAction<Pick<DocumentRevisionsConfig, "Name">>) => {
            const config = state.Config.Collections.find((x) => x.Name === payload.Name);
            config.Disabled = !config.Disabled;
        },
        deleteCollectionConfig: (state, { payload }: PayloadAction<Pick<DocumentRevisionsConfig, "Name">>) => {
            state.Config.Collections = state.Config.Collections.filter((x) => x.Name !== payload.Name);
        },
        editConflictsConfig: (state, { payload }: PayloadAction<DocumentRevisionsConfig>) => {
            state.ConflictsConfig = payload;
        },
        toggleConflictsConfig: (state) => {
            state.ConflictsConfig.Disabled = !state.ConflictsConfig.Disabled;
        },
        toggleSelectedConfigs: (
            state,
            { payload }: PayloadAction<{ names: DocumentRevisionsConfigName[]; disabled: boolean }>
        ) => {
            for (const name of payload.names) {
                if (name === "Conflicting Document Defaults") {
                    state.ConflictsConfig.Disabled = payload.disabled;
                    continue;
                }

                if (name === "Document Defaults") {
                    state.Config.Default.Disabled = payload.disabled;
                    continue;
                }

                const config = state.Config.Collections.find((x) => x.Name === name);
                config.Disabled = payload.disabled;
            }
        },
    },
    extraReducers: (builder) => {
        builder
            .addCase(fetchConfigs.fulfilled, (state, { payload }) => {
                if (payload.config) {
                    state.Config = {
                        Default: {
                            ...payload.config.Default,
                            Name: "Document Defaults",
                        },
                        Collections: Object.keys(payload.config.Collections).map((name) => ({
                            ...payload.config.Collections[name],
                            Name: name,
                        })),
                    };
                }

                if (payload.conflictsConfig) {
                    state.ConflictsConfig = {
                        ...payload.conflictsConfig,
                        Name: "Conflicting Document Defaults",
                    };
                }

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
>(sliceName + "/fetchConfig", async (db: database) => {
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
    state: (state: RootState) => state.documentRevisions,
};
