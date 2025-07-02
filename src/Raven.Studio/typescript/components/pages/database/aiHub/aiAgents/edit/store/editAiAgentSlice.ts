import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";

export interface EditAiAgentMessage {
    id: string;
    author: "user" | "agent";
    text?: string;
    date?: Date;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.Agents.AiUsage;
}

interface EditAiAgentState {
    isTestOpen: boolean;
    messages: EditAiAgentMessage[];
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
        messagesAdd: (state, action: PayloadAction<EditAiAgentMessage>) => {
            state.messages.push(action.payload);
        },
        messagesUpdate: (state, action: PayloadAction<Pick<EditAiAgentMessage, "id" | "state" | "text" | "usage">>) => {
            const message = state.messages.find((m) => m.id === action.payload.id);
            if (message) {
                message.state = action.payload.state;
                message.text = action.payload.text;
                message.usage = action.payload.usage;
            }
        },
    },
});

export const editAiAgentActions = editAiAgentSlice.actions;

export const editAiAgentSelectors = {
    isTestOpen: (state: RootState) => state.editAiAgent.isTestOpen,
    messages: (state: RootState) => state.editAiAgent.messages,
};
