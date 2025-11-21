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
import { StringWithAutocomplete } from "components/utils/common";
import { processStreamingResponse } from "components/utils/streamingUtils";
import router from "plugins/router";

type ChatbotTab = "Ask AI" | "What's new" | "News" | "Resources";
type ChatbotResourcesTab = "Help and resources" | "Join the community" | "Contact support" | "Submit feedback";
export type ChatbotUserActionState = "waiting" | "skipped" | "allowed";

type ChatbotRunChatData = {
    message?: string;
    actionResponses?: Record<string, any>;
};

export type ChatbotAttachedContextId = StringWithAutocomplete<
    "currentView" | "currentDatabaseName" | "currentIndexDefinition" | "currentDocument"
>;

export type ChatbotAttachedContext = {
    id: ChatbotAttachedContextId;
    type:
        | "Current View"
        | "Current Database Name"
        | "Current Index Definition"
        | "Current Document"
        | "Endpoints Responses";
    value: string;
    label: string;
    state: "included" | "excluded";
};

interface ChatbotMessageBase {
    id: string;
    content: string;
    role: "user" | "assistant";
}

export interface ChatbotUserMessage extends ChatbotMessageBase {
    role: "user";
    attachedContexts?: ChatbotAttachedContext[];
}

export interface ChatbotAssistantMessage extends ChatbotMessageBase {
    role: "assistant";
    thinkingTimeInMs: number;
    state: "Loading" | "Error" | AiAssistantResponseStatus;
    errorMessage?: string;
    usage: Raven.Client.Documents.Operations.AI.AiUsage;
    relevantLinks: RunChatbotAiAssistantResultDto["Response"]["RelevantLinks"];
    followUpQuestions: string[];
    endpoints: RunChatbotAiAssistantResultDto["Endpoints"];
    additionalContext: RunChatbotAiAssistantResultDto["AdditionalContext"];
    userActionState: ChatbotUserActionState;
}

export type ChatbotMessage = ChatbotUserMessage | ChatbotAssistantMessage;

interface ChatbotState {
    isOpen: boolean;
    isPinned: boolean;
    chatbotTab: ChatbotTab;
    chatbotResourcesTab: ChatbotResourcesTab;
    conversationId: string;
    messages: EntityState<ChatbotMessage, string>;
    lastRunData: ChatbotRunChatData;
    attachedContexts: EntityState<ChatbotAttachedContext, string>;
    deniedEndpoints: string[];
    alwaysAllowedEndpoints: string[];
}

const chatbotMessagesAdapter = createEntityAdapter<ChatbotMessage, string>({
    selectId: (message) => message.id,
});

const chatbotMessagesSelectors = chatbotMessagesAdapter.getSelectors();

const chatbotAttachedContextAdapter = createEntityAdapter<ChatbotAttachedContext, string>({
    selectId: (context) => context.id,
});

const chatbotAttachedContextSelectors = chatbotAttachedContextAdapter.getSelectors();

const initialState: ChatbotState = {
    isOpen: false,
    isPinned: false,
    chatbotTab: "Ask AI",
    chatbotResourcesTab: "Help and resources",
    conversationId: null,
    messages: chatbotMessagesAdapter.getInitialState(),
    lastRunData: null,
    deniedEndpoints: [],
    alwaysAllowedEndpoints: [],
    attachedContexts: chatbotAttachedContextAdapter.getInitialState(undefined, [
        {
            id: "currentView",
            type: "Current View",
            label: null,
            value: null,
            state: "included",
        },
        {
            id: "currentDatabaseName",
            type: "Current Database Name",
            label: null,
            value: null,
            state: "excluded",
        },
        {
            id: "currentIndexDefinition",
            type: "Current Index Definition",
            label: null,
            value: null,
            state: "excluded",
        },
        {
            id: "currentDocument",
            type: "Current Document",
            label: null,
            value: null,
            state: "excluded",
        },
    ]),
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
        lastRunDataSet: (state, action: PayloadAction<ChatbotRunChatData>) => {
            state.lastRunData = action.payload;
        },
        attachedContextRemoved: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.removeOne(state.attachedContexts, action.payload);
        },
        attachedContextAdded: (state, action: PayloadAction<ChatbotAttachedContext>) => {
            chatbotAttachedContextAdapter.addOne(state.attachedContexts, action.payload);
        },
        attachedContextSet: (
            state,
            action: PayloadAction<{ id: ChatbotAttachedContextId; label: string; value: string }>
        ) => {
            chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                id: action.payload.id,
                changes: {
                    value: action.payload.value,
                    label: action.payload.label,
                    state: "included",
                },
            });
        },
        attachedContextExcluded: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                id: action.payload,
                changes: { state: "excluded" },
            });
        },
        attachedContextIncluded: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                id: action.payload,
                changes: { state: "included" },
            });
        },
        attachedContextReset: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                id: action.payload,
                changes: { label: null, value: null, state: "excluded" },
            });
        },
        deniedEndpointsAdded: (state, action: PayloadAction<string[]>) => {
            state.deniedEndpoints = [...new Set([...state.deniedEndpoints, ...action.payload])];
        },
        alwaysAllowedEndpointsAdded: (state, action: PayloadAction<string[]>) => {
            state.alwaysAllowedEndpoints = [...new Set([...state.alwaysAllowedEndpoints, ...action.payload])];
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

        dispatch(chatbotActions.lastRunDataSet(payload));

        if (aiAssistant.consentStatus.data !== "Success") {
            throw new Error("AI Assistant consent is required");
        }

        const attachedContexts = chatbotAttachedContextSelectors
            .selectAll(chatbot.attachedContexts)
            .filter((context) => context.state === "included");

        const userMessage: ChatbotUserMessage = {
            id: _.uniqueId(),
            role: "user",
            content: payload.message,
            attachedContexts,
        };

        if (!payload.actionResponses) {
            if (chatbot.conversationId) {
                dispatch(chatbotActions.messageAdded(userMessage));
            } else {
                dispatch(chatbotActions.messagesSet([userMessage]));
            }
        }

        const responseId = _.uniqueId();
        const startThinkingTime = new Date().getTime();

        const assistantMessage: ChatbotAssistantMessage = {
            id: responseId,
            role: "assistant",
            content: "",
            state: "Loading",
            thinkingTimeInMs: null,
            usage: null,
            relevantLinks: [],
            followUpQuestions: [],
            endpoints: {},
            additionalContext: {},
            userActionState: null,
        };

        dispatch(chatbotActions.messageAdded(assistantMessage));

        const viewTitle = router.activeInstruction().config.title;

        const result = await processStreamingResponse<RunChatbotAiAssistantResultDto>({
            promiseFn: () =>
                services.aiAssistantService.runChatbot({
                    Message: payload.message,
                    View: viewTitle,
                    ConversationId: chatbot.conversationId,
                    ActionsResponses: payload.actionResponses,
                    AdditionalAttachedContext: Object.fromEntries(
                        attachedContexts.map((context) => [context.type, context.value])
                    ),
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

        const data = result.data;

        return {
            ...assistantMessage,
            state: data.Status,
            content: data.Response.Answer,
            thinkingTimeInMs: new Date().getTime() - startThinkingTime,
            relevantLinks: data.Response.RelevantLinks ?? [],
            followUpQuestions: data.Response.FollowUpQuestions ?? [],
            endpoints: data.Endpoints ?? {},
            additionalContext: data.AdditionalContext ?? {},
            userActionState: getUserActionState(data),
        };
    }
);

function getUserActionState(data: RunChatbotAiAssistantResultDto): ChatbotUserActionState {
    if (!_.isEmpty(data.AdditionalContext) || !_.isEmpty(data.Endpoints)) {
        return "waiting";
    }
    return null;
}

const retryRunChat = createAsyncThunk(chatbotSlice.name + "/retryRunChat", async (_, { dispatch, getState }) => {
    const { chatbot } = getState() as RootState;
    const { lastRunData } = chatbot;

    return await dispatch(runChat(lastRunData));
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
    oneBeforeLastMessageRole: (state: RootState) =>
        chatbotMessagesSelectors.selectById(
            state.chatbot.messages,
            chatbotMessagesSelectors.selectIds(state.chatbot.messages).at(-2)
        )?.role,
    messageById: (state: RootState, id: string) => chatbotMessagesSelectors.selectById(state.chatbot.messages, id),
    messagesCount: (state: RootState) => chatbotMessagesSelectors.selectTotal(state.chatbot.messages),
    isLastMessage: (state: RootState, id: string) =>
        chatbotMessagesSelectors.selectIds(state.chatbot.messages).at(-1) === id,
    conversationId: (state: RootState) => state.chatbot.conversationId,
    lastRunData: (state: RootState) => state.chatbot.lastRunData,
    attachedContexts: (state: RootState) => chatbotAttachedContextSelectors.selectAll(state.chatbot.attachedContexts),
    deniedEndpoints: (state: RootState) => state.chatbot.deniedEndpoints,
    alwaysAllowedEndpoints: (state: RootState) => state.chatbot.alwaysAllowedEndpoints,
};
