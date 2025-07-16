import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage, AiAgentRunResult, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData, loadStatus } from "components/models/common";
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";
import { EditAiAgentFormData } from "../utils/editAiAgentValidation";
import { editAiAgentUtils } from "../utils/editAiAgentUtils";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";

interface EditAiAgentState {
    isTestOpen: boolean;
    testMessages: AiAgentMessage[];
    testToolParameters: AiAgentToolCall[];
    testDocument: documentDto;
    isDocumentExpirationEnabled: loadableData<boolean>;
    runTestState: loadStatus;
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    testMessages: [],
    testToolParameters: [],
    testDocument: null,
    isDocumentExpirationEnabled: createIdleState(),
    runTestState: "idle",
};

export const editAiAgentSlice = createSlice({
    name: "editAiAgent",
    initialState,
    reducers: {
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        testMessagesAdd: (state, action: PayloadAction<AiAgentMessage>) => {
            state.testMessages.push(action.payload);
        },
        testMessagesUpdate: (state, action: PayloadAction<Partial<AiAgentMessage>>) => {
            const message = state.testMessages.find((m) => m.id === action.payload.id);
            if (message) {
                Object.assign(message, action.payload);
            }
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
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder.addCase(getIsDocumentExpirationEnabled.pending, (state) => {
            state.isDocumentExpirationEnabled.status = "loading";
        });
        builder.addCase(getIsDocumentExpirationEnabled.rejected, (state, action) => {
            state.isDocumentExpirationEnabled = createFailureState(action.error.message);
        });
        builder.addCase(getIsDocumentExpirationEnabled.fulfilled, (state, action) => {
            state.isDocumentExpirationEnabled = createSuccessState(action.payload);
        });
        builder.addCase(runTest.pending, (state) => {
            state.runTestState = "loading";
        });
        builder.addCase(runTest.rejected, (state) => {
            state.runTestState = "failure";
        });
        builder.addCase(runTest.fulfilled, (state, action) => {
            state.runTestState = "success";

            state.testDocument = action.payload.Document;

            const messages = action.payload.Document.Messages.map((x) => aiAgentsUtils.mapMessageFromDoc(x));
            state.testMessages = messages;
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
        payload: { databaseName: string; formValues: EditAiAgentFormData; toolCallParameters?: AiAgentToolCall[] },
        { getState }
    ): Promise<AiAgentRunResult> => {
        const { databaseName, formValues, toolCallParameters } = payload;

        const state = getState() as RootState;
        const testDocument = state.editAiAgent.testDocument;

        const result = await services.aiAgentService.testAiAgent(databaseName, {
            Configuration: editAiAgentUtils.mapToDto(formValues),
            UserPrompt: toolCallParameters?.length > 0 ? null : formValues.test.prompt,
            Parameters: Object.fromEntries(formValues.test.parameters.map((item) => [item.name, item.value])),
            ActionResponses: toolCallParameters?.map((x) => ({
                ToolId: x.id,
                Content: x.arguments,
            })),
            Document: testDocument,
            RequestBody: undefined,
        });

        return result;
    }
);
export const editAiAgentActions = {
    ...editAiAgentSlice.actions,
    getIsDocumentExpirationEnabled,
    runTest,
};

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    testMessages: (state: RootState) => state.editAiAgent.testMessages,
    testToolParameters: (state: RootState) => state.editAiAgent.testToolParameters,
    testDocument: (state: RootState) => state.editAiAgent.testDocument,
    isDocumentExpirationEnabled: (state: RootState) => state.editAiAgent.isDocumentExpirationEnabled,
    runTestState: (state: RootState) => state.editAiAgent.runTestState,
};
