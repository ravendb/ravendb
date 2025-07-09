import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage } from "../../partials/AiAgentMessages";

interface EditAiAgentState {
    isTestOpen: boolean;
    messages: AiAgentMessage[];
}

const initialState: EditAiAgentState = {
    isTestOpen: false,
    messages: [],
};

export const editAiAgentSlice = createSlice({
    name: "editAiAgent",
    initialState,
    reducers: {
        isTestOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isTestOpen = action.payload;
        },
        messagesAdd: (state, action: PayloadAction<AiAgentMessage>) => {
            state.messages.push(action.payload);
        },
        messagesUpdate: (state, action: PayloadAction<Pick<AiAgentMessage, "id" | "state" | "content" | "usage">>) => {
            const message = state.messages.find((m) => m.id === action.payload.id);
            if (message) {
                Object.assign(message, action.payload);
            }
        },
        reset: () => initialState,
    },
});

export const editAiAgentActions = editAiAgentSlice.actions;

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    messages: (state: RootState) => state.editAiAgent.messages,
};
