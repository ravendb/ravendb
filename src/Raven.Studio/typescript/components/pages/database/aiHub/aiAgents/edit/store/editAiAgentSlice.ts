import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData } from "components/models/common";
import { createFailureState, createIdleState, createSuccessState } from "components/utils/common";

interface EditAiAgentState {
    isTestOpen: boolean;
    testMessages: AiAgentMessage[];
    testToolParameters: AiAgentToolCall[];
    testDocument: any;
    isDocumentExpirationEnabled: loadableData<boolean>;
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    testMessages: [],
    testToolParameters: [],
    testDocument: null,
    isDocumentExpirationEnabled: createIdleState(),
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
        messagesUpdate: (state, action: PayloadAction<Partial<AiAgentMessage>>) => {
            const message = state.testMessages.find((m) => m.id === action.payload.id);
            if (message) {
                Object.assign(message, action.payload);
            }
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

export const editAiAgentActions = {
    ...editAiAgentSlice.actions,
    getIsDocumentExpirationEnabled,
};

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    testMessages: (state: RootState) => state.editAiAgent.testMessages,
    testToolParameters: (state: RootState) => state.editAiAgent.testToolParameters,
    testDocument: (state: RootState) => state.editAiAgent.testDocument,
    isDocumentExpirationEnabled: (state: RootState) => state.editAiAgent.isDocumentExpirationEnabled,
};
