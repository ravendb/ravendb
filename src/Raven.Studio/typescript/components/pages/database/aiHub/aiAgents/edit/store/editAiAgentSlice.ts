import { createAsyncThunk, createSelector, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadStatus } from "components/models/common";
import { TestAiAgentFormData } from "../utils/editAiAgentValidation";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";
import { aiAgentParametersUtils } from "../../utils/aiAgentParametersUtils";

const MAIN_TEST_CONVERSATION_ID = "TestConversation";

interface EditAiAgentState {
    isTestOpen: boolean;
    isTestPinned: boolean;
    isRawData: boolean;
    testToolParameters: AiAgentToolCall[];
    testDocuments: Record<string, documentDto>;
    testConfiguration: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
    runTestState: loadStatus;
    allIdentifiers?: string[];
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    isTestPinned: false,
    isRawData: false,
    testToolParameters: [],
    testDocuments: {},
    testConfiguration: null,
    runTestState: "idle",
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
        testToolParametersSet: (state, action: PayloadAction<AiAgentToolCall[]>) => {
            state.testToolParameters = action.payload;
        },
        testResultsReset: (state) => {
            state.testDocuments = {};
            state.testConfiguration = null;
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
            const { result, configuration } = action.payload;

            state.testDocuments = result.Documents ?? {};
            state.testConfiguration = configuration;

            state.runTestState = "success";
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
        result: Raven.Server.Documents.Handlers.AI.Agents.AiAgentProcessorForTestConversation.AiAgentTestResult;
        configuration: Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration;
    }> => {
        const { databaseName, configuration, testFormValues, toolCallParameters } = payload;

        const state = getState() as RootState;
        const testDocuments = state.editAiAgent.testDocuments;

        const result = await services.aiAgentService.testAiAgent(databaseName, {
            Configuration: configuration,
            UserPrompt: toolCallParameters?.length > 0 ? null : testFormValues.prompt,
            ActionResponses: toolCallParameters?.map((x) => ({
                ToolId: x.id,
                Content: x.arguments,
            })),
            Documents: testDocuments,
            RequestBody: undefined,
            CreationOptions: {
                Parameters: createParametersDto(testDocuments, testFormValues.parameters),
            },
        });

        return { result, configuration };
    }
);

function createParametersDto(
    testDocuments: Record<string, documentDto>,
    formParameters: TestAiAgentFormData["parameters"]
): Record<string, Raven.Client.Documents.AI.AiConversationParameter> {
    if (Object.keys(testDocuments ?? {}).length > 0) {
        return null;
    }

    return Object.fromEntries(
        formParameters.map((x) => [
            x.name,
            {
                Value: aiAgentParametersUtils.mapParameterValueToType(x.value, x.type),
                SendToModel: x.isSendToModel,
            },
        ])
    );
}

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

const selectTestDocuments = (state: RootState) => state.editAiAgent.testDocuments;
const selectTestConfiguration = (state: RootState) => state.editAiAgent.testConfiguration;

const selectMainTestDocument = createSelector([selectTestDocuments], (testDocuments) => {
    return testDocuments[MAIN_TEST_CONVERSATION_ID] ?? null;
});

const selectMainTestMessages = createSelector(
    [selectMainTestDocument, selectTestConfiguration],
    (conversationDocument, config) =>
        aiAgentsUtils.mapMessagesFromDoc({
            conversationDocument,
            config,
        })
);

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    isTestPinned: (state: RootState) => state.editAiAgent.isTestPinned,
    isRawData: (state: RootState) => state.editAiAgent.isRawData,
    testToolParameters: (state: RootState) => state.editAiAgent.testToolParameters,
    testDocuments: selectTestDocuments,
    mainTestDocument: selectMainTestDocument,
    mainTestMessages: selectMainTestMessages,
    testConfiguration: (state: RootState) => state.editAiAgent.testConfiguration,
    runTestState: (state: RootState) => state.editAiAgent.runTestState,
    isActionToolSubmitRequired: createSelector([selectMainTestDocument], (conversationDocument) =>
        aiAgentsUtils.hasOpenActionCalls(conversationDocument)
    ),
    allIdentifiers: (state: RootState) => state.editAiAgent.allIdentifiers,
};
