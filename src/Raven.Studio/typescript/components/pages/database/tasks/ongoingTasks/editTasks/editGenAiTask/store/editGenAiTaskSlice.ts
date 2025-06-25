import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { EditGenAiTaskStepId } from "../hooks/useEditGenAiTaskSteps";
import { services } from "components/hooks/useServices";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";

interface ModelUsage {
    totalTokens: number;
    promptTokens: number;
    completionTokens: number;
    cachedTokens: number;
}

interface EditGenAiTaskState {
    taskId: number;
    sourceView: EditAiTaskSourceView;
    currentStep: EditGenAiTaskStepId;
    connectionStringTest: loadableData<Raven.Server.Web.System.NodeConnectionTestResult>;
    contextTest: loadableData<string[]>;
    modelInputTest: loadableData<string[]>;
    modelUsage: loadableData<ModelUsage>;
    updateScriptTest: loadableData<string>;
    updateScriptDocumentInput: loadableData<string>;
    globalTestResult: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult;
    isPlaygroundCollapsed: boolean;
    isPlaygroundEditMode: boolean;
    aiConnectionStrings: Record<string, Raven.Client.Documents.Operations.AI.AiConnectionString>;
    isTestOpen: boolean;
    isDocumentInfoVisible: boolean;
    isContextInfoVisible: boolean;
    isModelInputInfoVisible: boolean;
    isEditModeWarningVisible: boolean;
    hoverIndex: number;
}

const initialState: EditGenAiTaskState = {
    taskId: null,
    sourceView: "OngoingTasks",
    currentStep: "basic",
    connectionStringTest: createIdleState(),
    contextTest: createIdleState([]),
    modelInputTest: createIdleState([]),
    modelUsage: createIdleState(),
    updateScriptTest: createIdleState(""),
    updateScriptDocumentInput: createIdleState(""),
    globalTestResult: null,
    isPlaygroundCollapsed: false,
    isPlaygroundEditMode: false,
    aiConnectionStrings: {},
    isTestOpen: false,
    isDocumentInfoVisible: true,
    isContextInfoVisible: true,
    isModelInputInfoVisible: true,
    isEditModeWarningVisible: true,
    hoverIndex: null,
};

export const editGenAiTaskSlice = createSlice({
    name: "editGenAiTask",
    initialState,
    reducers: {
        taskIdSet: (state, action: PayloadAction<number>) => {
            state.taskId = action.payload;
        },
        sourceViewSet: (state, action: PayloadAction<EditAiTaskSourceView>) => {
            state.sourceView = action.payload;
        },
        currentStepSet: (state, action: PayloadAction<EditGenAiTaskStepId>) => {
            state.currentStep = action.payload;
        },
        globalTestResultSet: (
            state,
            action: PayloadAction<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult>
        ) => {
            state.globalTestResult = action.payload;
        },
        isPlaygroundCollapsedToggled: (state) => {
            state.isPlaygroundCollapsed = !state.isPlaygroundCollapsed;
        },
        isPlaygroundEditModeToggled: (state) => {
            state.isPlaygroundEditMode = !state.isPlaygroundEditMode;
        },
        isPlaygroundEditModeSet: (state, action: PayloadAction<boolean>) => {
            state.isPlaygroundEditMode = action.payload;
        },
        aiConnectionStringsSet: (
            state,
            action: PayloadAction<Record<string, Raven.Client.Documents.Operations.AI.AiConnectionString>>
        ) => {
            state.aiConnectionStrings = action.payload;
        },
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        isDocumentInfoVisibleSet: (state, action: PayloadAction<boolean>) => {
            state.isDocumentInfoVisible = action.payload;
        },
        isContextInfoVisibleSet: (state, action: PayloadAction<boolean>) => {
            state.isContextInfoVisible = action.payload;
        },
        isModelInputInfoVisibleSet: (state, action: PayloadAction<boolean>) => {
            state.isModelInputInfoVisible = action.payload;
        },
        isEditModeWarningVisibleSet: (state, action: PayloadAction<boolean>) => {
            state.isEditModeWarningVisible = action.payload;
        },
        hoverIndexSet: (state, action: PayloadAction<number>) => {
            state.hoverIndex = action.payload;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(testContext.pending, (state) => {
                state.contextTest.status = "loading";
            })
            .addCase(testContext.rejected, (state, action) => {
                state.contextTest = createFailureState(action.error.message);
            })
            .addCase(testContext.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.contextTest = createSuccessState(
                    action.payload.Results.map((x) =>
                        x.ContextOutput ? JSON.stringify(x.ContextOutput.Context, null, 4) : null
                    )
                );
            })
            .addCase(testModelInput.pending, (state) => {
                state.modelInputTest.status = "loading";
                state.modelUsage.status = "loading";
            })
            .addCase(testModelInput.rejected, (state, action) => {
                state.modelInputTest = createFailureState(action.error.message);
                state.modelUsage = createFailureState(action.error.message);
            })
            .addCase(testModelInput.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.modelInputTest = createSuccessState(
                    action.payload.Results.map((x) =>
                        x.ModelOutput ? JSON.stringify(x.ModelOutput.Output, null, 4) : null
                    )
                );

                let totalTokens = 0;
                let promptTokens = 0;
                let completionTokens = 0;
                let cachedTokens = 0;

                for (const result of action.payload.Results) {
                    totalTokens += result.ModelOutput?.Usage.TotalTokens ?? 0;
                    promptTokens += result.ModelOutput?.Usage.PromptTokens ?? 0;
                    completionTokens += result.ModelOutput?.Usage.CompletionTokens ?? 0;
                    cachedTokens += result.ModelOutput?.Usage.CachedTokens ?? 0;
                }

                state.modelUsage = createSuccessState({
                    totalTokens,
                    promptTokens,
                    completionTokens,
                    cachedTokens,
                });
            })
            .addCase(testUpdateScript.pending, (state) => {
                state.updateScriptTest.status = "loading";
                state.updateScriptDocumentInput.status = "loading";
            })
            .addCase(testUpdateScript.rejected, (state, action) => {
                state.updateScriptTest = createFailureState(action.error.message);
                state.updateScriptDocumentInput = createFailureState(action.error.message);
            })
            .addCase(testUpdateScript.fulfilled, (state, action) => {
                state.globalTestResult = action.payload;
                state.isTestOpen = true;

                state.updateScriptTest = createSuccessState(
                    action.payload.OutputDocument ? JSON.stringify(action.payload.OutputDocument, null, 4) : null
                );

                state.updateScriptDocumentInput = createSuccessState(
                    action.payload.InputDocument ? JSON.stringify(action.payload.InputDocument, null, 4) : null
                );
            })
            .addCase(testConnectionString.pending, (state) => {
                state.connectionStringTest.status = "loading";
            })
            .addCase(testConnectionString.rejected, (state, action) => {
                state.connectionStringTest = createFailureState(action.error.message);
            })
            .addCase(testConnectionString.fulfilled, (state, action) => {
                state.connectionStringTest = createSuccessState(action.payload);
            });
    },
});

const testContext = createAsyncThunk(
    editGenAiTaskSlice.name + "/testContext",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

const testModelInput = createAsyncThunk(
    editGenAiTaskSlice.name + "/testModelInput",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

const testUpdateScript = createAsyncThunk(
    editGenAiTaskSlice.name + "/testUpdateScript",
    async (payload: {
        databaseName: string;
        dto: Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.TestGenAiScript;
    }): Promise<Raven.Server.Documents.ETL.Providers.AI.GenAi.Test.GenAiTestScriptResult> => {
        return services.tasksService.testGenAi(payload.databaseName, payload.dto);
    }
);

const testConnectionString = createAsyncThunk(
    editGenAiTaskSlice.name + "/testConnectionString",
    async (payload: {
        databaseName: string;
        connectorType: Raven.Client.Documents.Operations.AI.AiConnectorType;
        modelType: Raven.Client.Documents.Operations.AI.AiModelType;
        settings: AiConnectionStringsSettings;
    }): Promise<Raven.Server.Web.System.NodeConnectionTestResult> => {
        return services.tasksService.testAiConnectionString(
            payload.databaseName,
            payload.connectorType,
            payload.modelType,
            payload.settings
        );
    }
);

export const editGenAiTaskActions = {
    ...editGenAiTaskSlice.actions,
    testContext,
    testModelInput,
    testUpdateScript,
    testConnectionString,
};

export const editGenAiTaskSelectors = {
    taskId: (state: RootState) => state.editGenAiTask.taskId,
    isNewTask: (state: RootState) => state.editGenAiTask.taskId == null,
    isEditTask: (state: RootState) => state.editGenAiTask.taskId != null,
    sourceView: (state: RootState) => state.editGenAiTask.sourceView,
    currentStep: (state: RootState) => state.editGenAiTask.currentStep,
    isTestOpen: (state: RootState) => state.editGenAiTask.isTestOpen,
    connectionStringTest: (state: RootState) => state.editGenAiTask.connectionStringTest,
    contextTest: (state: RootState) => state.editGenAiTask.contextTest,
    modelInputTest: (state: RootState) => state.editGenAiTask.modelInputTest,
    updateScriptTest: (state: RootState) => state.editGenAiTask.updateScriptTest,
    updateScriptDocumentInput: (state: RootState) => state.editGenAiTask.updateScriptDocumentInput,
    modelUsage: (state: RootState) => state.editGenAiTask.modelUsage,
    isPlaygroundCollapsed: (state: RootState) => state.editGenAiTask.isPlaygroundCollapsed,
    isPlaygroundEditMode: (state: RootState) => state.editGenAiTask.isPlaygroundEditMode,
    globalTestResult: (state: RootState) => state.editGenAiTask.globalTestResult,
    aiConnectionStrings: (state: RootState) => state.editGenAiTask.aiConnectionStrings,
    isDocumentInfoVisible: (state: RootState) => state.editGenAiTask.isDocumentInfoVisible,
    isContextInfoVisible: (state: RootState) => state.editGenAiTask.isContextInfoVisible,
    isModelInputInfoVisible: (state: RootState) => state.editGenAiTask.isModelInputInfoVisible,
    isEditModeWarningVisible: (state: RootState) => state.editGenAiTask.isEditModeWarningVisible,
    hoverIndex: (state: RootState) => state.editGenAiTask.hoverIndex,
};
