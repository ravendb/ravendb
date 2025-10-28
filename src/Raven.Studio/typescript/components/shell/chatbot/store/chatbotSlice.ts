import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import {
    RunChatbotAiAssistantResultDto,
    RunChatbotAiAssistantViewData,
} from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";

export type ChatbotRole = "user" | "assistant";
type ChatbotTab = "Ask AI" | "What's new" | "News" | "Resources";
type ChatbotResourcesTab = "Help and resources" | "Join the community" | "Contact support" | "Submit feedback";

type ChatbotRunChatData = Omit<RunChatbotAiAssistantViewData, "RavenVersion">;

export interface ChatbotMessage {
    id: string;
    role: ChatbotRole;
    content?: string;
    thinkingTimeInMs?: number;
    state?: "loading" | "success" | "error";
    usage?: Raven.Client.Documents.Operations.AI.AiUsage;
    relevantLinks?: RunChatbotAiAssistantResultDto["Response"]["RelevantLinks"];
}

interface ChatbotState {
    isOpen: boolean;
    isPinned: boolean;
    chatbotTab: ChatbotTab;
    chatbotResourcesTab: ChatbotResourcesTab;
    conversationId: string;
    messages: ChatbotMessage[];
    absoluteNotificationsWidth: number;
    lastRunChatData: ChatbotRunChatData;
}

const initialState: ChatbotState = {
    isOpen: false,
    isPinned: false,
    chatbotTab: "Ask AI",
    chatbotResourcesTab: "Help and resources",
    conversationId: null,
    messages: [],
    absoluteNotificationsWidth: 0,
    lastRunChatData: null,
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
        conversationIdSet: (state, action: PayloadAction<string>) => {
            state.conversationId = action.payload;
        },
        lastRunChatDataSet: (state, action: PayloadAction<ChatbotRunChatData>) => {
            state.lastRunChatData = action.payload;
        },
        absoluteNotificationsWidthSet: (state, action: PayloadAction<number>) => {
            state.absoluteNotificationsWidth = action.payload;
        },
    },
    extraReducers: (builder) => {
        builder.addCase(runChat.fulfilled, (state, { payload }) => {
            if ("failedResponseId" in payload) {
                state.messages = state.messages.map((x) =>
                    x.id === payload.failedResponseId ? { ...x, state: "error" } : x
                );
            } else {
                state.messages = state.messages.map((x) => (x.id === payload.id ? payload : x));
            }
        });
    },
});

const runChat = createAsyncThunk(
    chatbotSlice.name + "/runChat",
    async (
        payload: { data: ChatbotRunChatData },
        { dispatch, getState }
    ): Promise<ChatbotMessage | { failedResponseId: string }> => {
        const { aiAssistant, chatbot, cluster } = getState() as RootState;

        dispatch(chatbotActions.lastRunChatDataSet(payload.data));

        if (aiAssistant.consentStatus.data !== "Success") {
            throw new Error("AI Assistant consent is required");
        }

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

        if (chatbot.conversationId) {
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

        try {
            const result = await services.aiAssistantService.runChatbot({
                View: payload.data.View,
                Message: payload.data.Message,
                ConversationId: chatbot.conversationId,
                RavenVersion: cluster.serverVersion?.ProductVersion ?? "latest",
            });

            dispatch(chatbotActions.conversationIdSet(result.ConversationId));

            return {
                ...assistantMessage,
                content: result.Response.Answer,
                state: "success",
                thinkingTimeInMs: new Date().getTime() - startThinkingTime,
                usage: {
                    TotalTokens: result.InputTokenCount,
                    PromptTokens: result.InputTokenCount,
                    CompletionTokens: result.OutputTokenCount,
                    CachedTokens: 0, // TODO server-side
                },
                relevantLinks: result.Response.RelevantLinks,
            };
        } catch {
            return {
                failedResponseId: responseId,
            };
        }
    }
);

const retryRunChat = createAsyncThunk(chatbotSlice.name + "/retryRunChat", async (_, { dispatch, getState }) => {
    const { chatbot } = getState() as RootState;
    const { lastRunChatData } = chatbot;

    return await dispatch(runChat({ data: lastRunChatData }));
});

export const chatbotActions = {
    ...chatbotSlice.actions,
    runChat,
    retryRunChat,
};

export const chatbotSelectors = {
    isOpen: (state: RootState) => state.chatbot.isOpen,
    isPinned: (state: RootState) => state.chatbot.isPinned,
    chatbotTab: (state: RootState) => state.chatbot.chatbotTab,
    chatbotResourcesTab: (state: RootState) => state.chatbot.chatbotResourcesTab,
    messages: (state: RootState) => state.chatbot.messages,
    conversationId: (state: RootState) => state.chatbot.conversationId,
    absoluteNotificationsWidth: (state: RootState) => state.chatbot.absoluteNotificationsWidth,
    lastRunChatData: (state: RootState) => state.chatbot.lastRunChatData,
};
