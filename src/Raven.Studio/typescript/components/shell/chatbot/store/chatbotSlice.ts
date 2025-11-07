import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    EntityState,
    PayloadAction,
    Update,
} from "@reduxjs/toolkit";
import { RunChatbotAiAssistantResultDto } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { aiAssistantActions } from "components/common/shell/aiAssistantSlice";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";
import { processStreamingResponse } from "components/utils/streamingUtils";
import router from "plugins/router";

type ChatbotTab = "Ask AI" | "What's new" | "News" | "Resources";
type ChatbotResourcesTab = "Help and resources" | "Join the community" | "Contact support" | "Submit feedback";

interface ChatbotRunChatData {
    message: string;
}

interface ChatbotMessageBase {
    id: string;
    content: string;
    role: "user" | "assistant";
}

export interface ChatbotUserMessage extends ChatbotMessageBase {
    role: "user";
}

export interface ChatbotAssistantMessage extends ChatbotMessageBase {
    role: "assistant";
    thinkingTimeInMs: number;
    state: "Loading" | "Error" | AiAssistantResponseStatus;
    errorMessage?: string;
    usage: Raven.Client.Documents.Operations.AI.AiUsage;
    relevantLinks: RunChatbotAiAssistantResultDto["Response"]["RelevantLinks"];
    followUpQuestions: string[];
}

export type ChatbotMessage = ChatbotUserMessage | ChatbotAssistantMessage;

interface ChatbotState {
    isOpen: boolean;
    isPinned: boolean;
    chatbotTab: ChatbotTab;
    chatbotResourcesTab: ChatbotResourcesTab;
    conversationId: string;
    messages: EntityState<ChatbotMessage, string>;
    runChatLastMessage: string;
    isNotificationsVisibleAndPinned: boolean;
}

const chatbotMessagesAdapter = createEntityAdapter<ChatbotMessage, string>({
    selectId: (message) => message.id,
});

const chatbotMessagesSelectors = chatbotMessagesAdapter.getSelectors();

const initialState: ChatbotState = {
    isOpen: false,
    isPinned: false,
    chatbotTab: "Ask AI",
    chatbotResourcesTab: "Help and resources",
    conversationId: null,
    messages: chatbotMessagesAdapter.getInitialState(),
    runChatLastMessage: null,
    isNotificationsVisibleAndPinned: false,
};

export const chatbotSlice = createSlice({
    name: "chatbot",
    initialState,
    reducers: {
        isOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isOpen = action.payload;
        },
        isOpenToggled: (state) => {
            state.isOpen = !state.isOpen;
        },
        isPinnedToggled: (state) => {
            state.isPinned = !state.isPinned;
        },
        chatbotTabSet: (state, action: PayloadAction<ChatbotTab>) => {
            state.chatbotTab = action.payload;
        },
        chatbotResourcesTabSet: (state, action: PayloadAction<ChatbotResourcesTab>) => {
            state.chatbotResourcesTab = action.payload;
        },
        messagesSet: (state, action: PayloadAction<ChatbotMessage[]>) => {
            chatbotMessagesAdapter.setAll(state.messages, action.payload);
        },
        messageAdded: (state, action: PayloadAction<ChatbotMessage>) => {
            chatbotMessagesAdapter.addOne(state.messages, action.payload);
        },
        messageUpdated: (state, action: PayloadAction<Update<ChatbotMessage, string>>) => {
            chatbotMessagesAdapter.updateOne(state.messages, action.payload);
        },
        conversationIdSet: (state, action: PayloadAction<string>) => {
            state.conversationId = action.payload;
        },
        runChatLastMessageSet: (state, action: PayloadAction<string>) => {
            state.runChatLastMessage = action.payload;
        },
        isNotificationsVisibleAndPinnedSet: (state, action: PayloadAction<boolean>) => {
            state.isNotificationsVisibleAndPinned = action.payload;
        },
    },
    extraReducers: (builder) => {
        builder.addCase(runChat.fulfilled, (state, { payload }) => {
            chatbotMessagesAdapter.updateOne(state.messages, {
                id: payload.id,
                changes: payload,
            });
        });
    },
});

const runChat = createAsyncThunk(
    chatbotSlice.name + "/runChat",
    async (payload: ChatbotRunChatData, { dispatch, getState }): Promise<ChatbotMessage> => {
        const { aiAssistant, chatbot } = getState() as RootState;

        dispatch(chatbotActions.runChatLastMessageSet(payload.message));

        if (aiAssistant.consentStatus.data !== "Success") {
            throw new Error("AI Assistant consent is required");
        }

        const userMessage: ChatbotUserMessage = {
            id: _.uniqueId(),
            role: "user",
            content: payload.message,
        };

        if (chatbot.conversationId) {
            dispatch(chatbotActions.messageAdded(userMessage));
        } else {
            dispatch(chatbotActions.messagesSet([userMessage]));
        }

        const responseId = _.uniqueId();
        const startThinkingTime = new Date().getTime();

        const assistantMessage: ChatbotAssistantMessage = {
            id: responseId,
            role: "assistant",
            content: "",
            state: "Loading",
            thinkingTimeInMs: 0,
            usage: null,
            relevantLinks: [],
            followUpQuestions: [],
        };

        dispatch(chatbotActions.messageAdded(assistantMessage));

        const viewTitle = router.activeInstruction().config.title;

        const result = await processStreamingResponse<RunChatbotAiAssistantResultDto>({
            promiseFn: () =>
                services.aiAssistantService.runChatbot({
                    Message: payload.message,
                    View: viewTitle,
                    ConversationId: chatbot.conversationId,
                }),
            streamPropertyPath: "Response.Answer",
            onChunksCombined(text) {
                dispatch(
                    chatbotActions.messageUpdated({
                        id: responseId,
                        changes: { state: "Success", content: text },
                    })
                );
            },
        });

        if (result.status === "error") {
            return {
                ...assistantMessage,
                state: "Error",
                errorMessage: result.error,
            };
        }

        dispatch(aiAssistantActions.usagePercentageSet(result.data.UsagePercentage));
        dispatch(chatbotActions.conversationIdSet(result.data.ConversationId));

        return {
            ...assistantMessage,
            state: result.data.Status,
            content: result.data.Response.Answer,
            thinkingTimeInMs: new Date().getTime() - startThinkingTime,
            relevantLinks: result.data.Response.RelevantLinks,
            followUpQuestions: result.data.Response.FollowUpQuestions,
        };
    }
);

const retryRunChat = createAsyncThunk(chatbotSlice.name + "/retryRunChat", async (_, { dispatch, getState }) => {
    const { chatbot } = getState() as RootState;
    const { runChatLastMessage } = chatbot;

    return await dispatch(runChat({ message: runChatLastMessage }));
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
    messageIds: (state: RootState) => chatbotMessagesSelectors.selectIds(state.chatbot.messages),
    messageById: (state: RootState, id: string) => chatbotMessagesSelectors.selectById(state.chatbot.messages, id),
    messagesCount: (state: RootState) => chatbotMessagesSelectors.selectTotal(state.chatbot.messages),
    isLastMessage: (state: RootState, id: string) =>
        chatbotMessagesSelectors.selectIds(state.chatbot.messages).at(-1) === id,
    conversationId: (state: RootState) => state.chatbot.conversationId,
    runChatLastMessage: (state: RootState) => state.chatbot.runChatLastMessage,
    isNotificationsVisibleAndPinned: (state: RootState) => state.chatbot.isNotificationsVisibleAndPinned,
};
