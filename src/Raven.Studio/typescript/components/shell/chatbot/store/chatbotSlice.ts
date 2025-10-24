import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RunChatbotAiAssistantViewData } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";

export type ChatbotRole = "system" | "user" | "assistant" | "tool";
type ChatbotTab = "askAi" | "whatsNew" | "news" | "resources";
type ChatbotResourcesTab = "Help and resources" | "Join the community" | "Contact support" | "Submit feedback";

export interface ChatbotMessage {
    id: string;
    role: ChatbotRole;
    content?: string;
    thinkingTimeInMs?: number;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.AiUsage;
}

interface ChatbotState {
    isOpen: boolean;
    isPinned: boolean;
    chatbotTab: ChatbotTab;
    chatbotResourcesTab: ChatbotResourcesTab;
    messages: ChatbotMessage[];
    absoluteNotificationsWidth: number;
}

const initialState: ChatbotState = {
    isOpen: false, // TODO change to false
    isPinned: false,
    chatbotTab: "askAi",
    chatbotResourcesTab: "Help and resources",
    messages: [],
    absoluteNotificationsWidth: 0,
};

export const chatbotSlice = createSlice({
    name: "chatbot",
    initialState,
    reducers: {
        isOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isOpen = action.payload;
        },
        isPinnedSet: (state, action: PayloadAction<boolean>) => {
            state.isPinned = action.payload;
        },
        chatbotTabSet: (state, action: PayloadAction<ChatbotTab>) => {
            state.chatbotTab = action.payload;
        },
        chatbotResourcesTabSet: (state, action: PayloadAction<ChatbotResourcesTab>) => {
            state.chatbotResourcesTab = action.payload;
        },
        messagesSet: (state, action: PayloadAction<ChatbotMessage[]>) => {
            state.messages = action.payload;
        },
        messageAdded: (state, action: PayloadAction<ChatbotMessage>) => {
            state.messages.push(action.payload);
        },
        absoluteNotificationsWidthSet: (state, action: PayloadAction<number>) => {
            state.absoluteNotificationsWidth = action.payload;
        },
    },
    extraReducers: (builder) => {
        builder.addCase(runChat.fulfilled, (state, action) => {
            state.messages = state.messages.map((x) => (x.id === action.payload.id ? action.payload : x));
        });
    },
});

const runChat = createAsyncThunk(
    chatbotSlice.name + "/runChat",
    async (
        payload: { data: RunChatbotAiAssistantViewData; conversationId?: string },
        { dispatch }
    ): Promise<ChatbotMessage> => {
        const userId = _.uniqueId();

        const userMessage: ChatbotMessage = {
            id: userId,
            role: "user",
            content: payload.data.Message,
            state: "success",
            usage: {
                TotalTokens: 100,
                PromptTokens: 100,
                CompletionTokens: 100,
                CachedTokens: 100,
            },
        };

        if (payload.conversationId) {
            dispatch(chatbotActions.messageAdded(userMessage));
        } else {
            dispatch(chatbotActions.messagesSet([userMessage]));
        }

        const responseId = _.uniqueId();
        const startThinkingTime = new Date().getTime();

        const assistantMessage: ChatbotMessage = {
            id: responseId,
            role: "assistant",
            state: "loading",
        };

        dispatch(chatbotActions.messageAdded(assistantMessage));

        const result = await services.aiAssistantService.runChatbot({
            View: payload.data.View,
            Message: payload.data.Message,
        });

        console.log("kalczur result", result);

        return {
            ...assistantMessage,
            content: mockedResponseContent,
            state: "success",
            thinkingTimeInMs: new Date().getTime() - startThinkingTime,
            usage: {
                TotalTokens: 100,
                PromptTokens: 100,
                CompletionTokens: 100,
                CachedTokens: 100,
            },
        };
    }
);

const mockedResponseContent = `Indexes are server-side, persisted structures that precompute queryable fields so reads don’t scan entire collections. RavenDB creates auto-indexes for ad-hoc queries; for recurring/high-traffic patterns, use static indexes - Map for filtering/sorting and Map-Reduce for pre-aggregations. They update asynchronously as documents change (with optional staleness control).

Recommendation: identify your top queries (filters, sorts, aggregates) and create static indexes for those; use Map-Reduce where you need rollups (e.g., per day/month totals).`;

export const chatbotActions = {
    ...chatbotSlice.actions,
    runChat,
};

export const chatbotSelectors = {
    isOpen: (state: RootState) => state.chatbot.isOpen,
    isPinned: (state: RootState) => state.chatbot.isPinned,
    chatbotTab: (state: RootState) => state.chatbot.chatbotTab,
    chatbotResourcesTab: (state: RootState) => state.chatbot.chatbotResourcesTab,
    messages: (state: RootState) => state.chatbot.messages,
    absoluteNotificationsWidth: (state: RootState) => state.chatbot.absoluteNotificationsWidth,
};
