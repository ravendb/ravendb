import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";

interface EditAiAgentState {
    isTestOpen: boolean;
    testMessages: AiAgentMessage[];
    testToolParameters: AiAgentToolCall[];
    testDocument: any;
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    testMessages: [],
    testToolParameters: [],
    testDocument: null,
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
});

export const editAiAgentActions = editAiAgentSlice.actions;

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    testMessages: (state: RootState) => state.editAiAgent.testMessages,
    testToolParameters: (state: RootState) => state.editAiAgent.testToolParameters,
    testDocument: (state: RootState) => state.editAiAgent.testDocument,
};
