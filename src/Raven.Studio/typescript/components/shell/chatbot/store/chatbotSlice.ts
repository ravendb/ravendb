import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    EntityState,
    PayloadAction,
    Update,
} from "@reduxjs/toolkit";
import { RunChatbotAiAssistantResultDto } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";
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
    state: "loading" | "success" | "error";
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
    absoluteNotificationsWidth: number;
    runChatLastMessage: string;
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
    absoluteNotificationsWidth: 0,
    runChatLastMessage: null,
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
        absoluteNotificationsWidthSet: (state, action: PayloadAction<number>) => {
            state.absoluteNotificationsWidth = action.payload;
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
            state: "loading",
            thinkingTimeInMs: 0,
            usage: null,
            relevantLinks: [],
            followUpQuestions: [],
        };

        dispatch(chatbotActions.messageAdded(assistantMessage));

        try {
            const viewTitle = router.activeInstruction().config.title;

            const response = await services.aiAssistantService.runChatbot({
                Message: payload.message,
                View: viewTitle,
                ConversationId: chatbot.conversationId,
            });

            const reader = response.body.getReader();
            const decoder = new TextDecoder("utf-8");

            let content = "";

            for (;;) {
                const { done, value } = await reader.read();
                if (done) {
                    break;
                }

                const responseString = decoder.decode(value, { stream: true });
                const responseLines = responseString.split("\n");

                for (const line of responseLines) {
                    if (!line.startsWith("data: ")) {
                        continue;
                    }

                    const dataString = line.trim().replace("data: ", "");
                    const data: { text: string | RunChatbotAiAssistantResultDto; done: boolean } =
                        JSON.parse(dataString);

                    if (!data.done && typeof data.text === "string") {
                        content += data.text;

                        dispatch(
                            chatbotActions.messageUpdated({
                                id: responseId,
                                changes: {
                                    state: "success",
                                    content,
                                },
                            })
                        );
                    }

                    if (data.done && typeof data.text === "object") {
                        const finalData = data.text;

                        dispatch(chatbotActions.conversationIdSet(finalData.ConversationId));

                        return {
                            ...assistantMessage,
                            content,
                            state: "success",
                            thinkingTimeInMs: new Date().getTime() - startThinkingTime,
                            usage: {
                                TotalTokens: finalData.InputTokenCount,
                                PromptTokens: finalData.InputTokenCount,
                                CompletionTokens: finalData.OutputTokenCount,
                                CachedTokens: 0, // TODO server-side
                                ReasoningTokens: 0, // TODO server-side
                            },
                            relevantLinks: finalData.Response.RelevantLinks,
                            followUpQuestions: finalData.Response.FollowUpQuestions,
                        };
                    }
                }
            }

            console.error("Failed to finish the AI Assistant response");
            return {
                ...assistantMessage,
                state: "error",
            };
        } catch (e) {
            console.error(e);
            return {
                ...assistantMessage,
                state: "error",
            };
        }
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
    isLastMessage: (state: RootState, id: string) => {
        const messageIds = chatbotMessagesSelectors.selectIds(state.chatbot.messages);
        return messageIds[messageIds.length - 1] === id;
    },
    conversationId: (state: RootState) => state.chatbot.conversationId,
    absoluteNotificationsWidth: (state: RootState) => state.chatbot.absoluteNotificationsWidth,
    runChatLastMessage: (state: RootState) => state.chatbot.runChatLastMessage,
};
