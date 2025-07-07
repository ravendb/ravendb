import { createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage } from "../../partials/AiAgentMessages";

interface EditAiAgentState {
    prompt: string;
    chatId: string;
    messages: AiAgentMessage[];
}

const initialState: EditAiAgentState = {
    prompt: "",
    chatId: "",
    messages: [],
};

export const chatAiAgentSlice = createSlice({
    name: "chatAiAgent",
    initialState,
    reducers: {
        promptSet: (state, action: PayloadAction<string>) => {
            state.prompt = action.payload;
        },
        chatIdSet: (state, action: PayloadAction<string>) => {
            state.chatId = action.payload;
        },
        messagesAdd: (state, action: PayloadAction<AiAgentMessage>) => {
            state.messages.push(action.payload);
        },
        messagesUpdate: (state, action: PayloadAction<Pick<AiAgentMessage, "id" | "state" | "text" | "usage">>) => {
            const message = state.messages.find((m) => m.id === action.payload.id);
            if (message) {
                Object.assign(message, action.payload);
            }
        },
        reset: () => initialState,
    },
});

export const chatAiAgentActions = chatAiAgentSlice.actions;

export const chatAiAgentSelectors = {
    prompt: (state: RootState) => state.chatAiAgent.prompt,
    messages: (state: RootState) => state.chatAiAgent.messages,
    chatId: (state: RootState) => state.chatAiAgent.chatId,
};
