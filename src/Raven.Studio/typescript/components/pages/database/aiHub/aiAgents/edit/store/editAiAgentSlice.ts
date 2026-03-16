import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage, AiAgentRunResult, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import { TestAiAgentFormData } from "../utils/editAiAgentValidation";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";

interface EditAiAgentState {
    isTestOpen: boolean;
    isTestPinned: boolean;
    isRawData: boolean;
    testMessages: AiAgentMessage[];
    testToolParameters: AiAgentToolCall[];
    testDocument: documentDto;
    runTestState: loadStatus;
    isWaitingForActionToolSubmit: boolean;
    allIdentifiers?: string[];
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    isTestPinned: false,
    isRawData: false,
    testMessages: [],
    testToolParameters: [],
    testDocument: null,
    runTestState: "idle",
    isWaitingForActionToolSubmit: false,
    allIdentifiers: [],
};

export const editAiAgentSlice = createSlice({
    name: "editAiAgent",
    initialState,
    reducers: {
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        isTestPinnedSet: (state, action: PayloadAction<boolean>) => {
            state.isTestPinned = action.payload;
        },
        isRawDataSet: (state, action: PayloadAction<boolean>) => {
            state.isRawData = action.payload;
        },
        testMessagesSet: (state, action: PayloadAction<AiAgentMessage[]>) => {
            state.testMessages = action.payload;
        },
        testToolParametersSet: (state, action: PayloadAction<AiAgentToolCall[]>) => {
            state.testToolParameters = action.payload;
        },
        testDocumentSet: (state, action: PayloadAction<any>) => {
            state.testDocument = action.payload;
        },
        isWaitingForActionToolSubmitSet: (state, action: PayloadAction<boolean>) => {
            state.isWaitingForActionToolSubmit = action.payload;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder.addCase(runTest.pending, (state) => {
            state.runTestState = "loading";
        });
        builder.addCase(runTest.rejected, (state) => {
            state.runTestState = "failure";
        });
        builder.addCase(runTest.fulfilled, (state, action) => {
            state.runTestState = "success";
            state.testDocument = action.payload.result.Document;

            const { result, configuration } = action.payload;

            state.testMessages = aiAgentsUtils.mapMessagesFromDoc({
                docMessages: result.Document.Messages,
                config: configuration,
            });
        });
        builder.addCase(getAllIdentifiers.fulfilled, (state, action) => {
            state.allIdentifiers = action.payload;
        });
    },
});

const getIsDocumentExpirationEnabled = createAsyncThunk(
    editAiAgentSlice.name + "/getIsDocumentExpirationEnabled",
    async (databaseName: string): Promise<boolean> => {
        const result = await services.databasesService.getExpirationConfiguration(databaseName);
        if (!result) {
            return false;
        }
        return !result.Disabled;
    }
);

const runTest = createAsyncThunk(
    editAiAgentSlice.name + "/runTest",
    async (
        payload: {
            databaseName: string;
            configuration: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
            testFormValues: TestAiAgentFormData;
            toolCallParameters?: AiAgentToolCall[];
        },
        { getState }
    ): Promise<{
        result: AiAgentRunResult;
        configuration: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
    }> => {
        const { databaseName, configuration, testFormValues, toolCallParameters } = payload;

        const state = getState() as RootState;
        const testDocument = state.editAiAgent.testDocument;

        const result = await services.aiAgentService.testAiAgent(databaseName, {
            Configuration: configuration,
            UserPrompt: toolCallParameters?.length > 0 ? null : testFormValues.prompt,
            ActionResponses: toolCallParameters?.map((x) => ({
                ToolId: x.id,
                Content: x.arguments,
            })),
            Document: testDocument,
            RequestBody: undefined,
            CreationOptions: {
                Parameters: Object.fromEntries(testFormValues.parameters.map((item) => [item.name, item.value])),
            },
        });

        return { result, configuration };
    }
);

const getAllIdentifiers = createAsyncThunk(
    editAiAgentSlice.name + "/getAllIdentifiers",
    async (databaseName: string): Promise<string[]> => {
        const result = await services.aiAgentService.getAiAgents(databaseName);
        if (!result) {
            return [];
        }

        return result.AiAgents.map((agent) => agent.Identifier);
    }
);

export const editAiAgentActions = {
    ...editAiAgentSlice.actions,
    getAllIdentifiers,
    getIsDocumentExpirationEnabled,
    runTest,
};

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    isTestPinned: (state: RootState) => state.editAiAgent.isTestPinned,
    isRawData: (state: RootState) => state.editAiAgent.isRawData,
    testMessages: (state: RootState) => state.editAiAgent.testMessages,
    testToolParameters: (state: RootState) => state.editAiAgent.testToolParameters,
    testDocument: (state: RootState) => state.editAiAgent.testDocument,
    runTestState: (state: RootState) => state.editAiAgent.runTestState,
    isWaitingForActionToolSubmit: (state: RootState) => state.editAiAgent.isWaitingForActionToolSubmit,
    allIdentifiers: (state: RootState) => state.editAiAgent.allIdentifiers,
};
