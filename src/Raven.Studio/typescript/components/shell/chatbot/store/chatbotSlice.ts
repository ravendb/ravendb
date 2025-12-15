import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    EntityState,
    PayloadAction,
    Update,
} from "@reduxjs/toolkit";
import { RunChatbotAiAssistantResultDto } from "commands/aiAssistant/runChatbotAiAssistantCommand";
import fileDownloader from "common/fileDownloader";
import { aiAssistantActions } from "components/common/shell/aiAssistantSlice";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";
import { processStreamingResponse } from "components/utils/aiAssistStreamingUtils";
import moment from "moment";

type ChatbotTab = "askAi" | "resources";
type ChatbotResourcesTab = "helpAndResources" | "joinTheCommunity" | "contactSupport" | "submitFeedback";
export type ChatbotUserActionState = "waiting" | "allowed" | "alwaysAllowed" | "skipped" | "denied" | "error";

interface ChatbotRunChatData {
    message?: string;
    actionResponses?: Record<string, any>;
}

export type ChatbotAttachedContextId =
    | "View"
    | "DatabaseName"
    | "DocumentId"
    | "CollectionName"
    | "IndexName"
    | `QueryResult-${string}`
    | `QueryError-${string}`;

export interface ChatbotAttachedContext {
    id: ChatbotAttachedContextId;
    type: "View" | "DatabaseName" | "IndexName" | "CollectionName" | "DocumentId" | "QueryResult" | "QueryError";
    value: any;
    label: string;
    state: "included" | "excluded";
    query?: string;
}

export interface ChatbotEndpointItem {
    toolId: string;
    url: string;
    state: ChatbotUserActionState;
}

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
    endpoints: ChatbotEndpointItem[];
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
    attachedContexts: EntityState<ChatbotAttachedContext, ChatbotAttachedContextId>;
    isNewContextOpen: boolean;
    newContextTab: ChatbotAttachedContext["type"];
    deniedEndpoints: string[];
    isAlwaysAllowEndpointCalls: boolean;
    isRunQueryFromChatbot: boolean;
}

const chatbotMessagesAdapter = createEntityAdapter<ChatbotMessage, string>({
    selectId: (message) => message.id,
});

const chatbotMessagesSelectors = chatbotMessagesAdapter.getSelectors();

const chatbotAttachedContextAdapter = createEntityAdapter<ChatbotAttachedContext, ChatbotAttachedContextId>({
    selectId: (context) => context.id,
});

const chatbotAttachedContextSelectors = chatbotAttachedContextAdapter.getSelectors();

const initialState: ChatbotState = {
    isOpen: false,
    isPinned: true,
    chatbotTab: "askAi",
    chatbotResourcesTab: "helpAndResources",
    conversationId: null,
    messages: chatbotMessagesAdapter.getInitialState(),
    lastRunData: null,
    deniedEndpoints: [],
    attachedContexts: chatbotAttachedContextAdapter.getInitialState(),
    isNewContextOpen: false,
    newContextTab: null,
    isAlwaysAllowEndpointCalls: true,
    isRunQueryFromChatbot: false,
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
        attachedContextUpserted: (state, action: PayloadAction<ChatbotAttachedContext, "id">) => {
            const { id, ...rest } = action.payload;
            const isExisting = chatbotAttachedContextSelectors.selectById(state.attachedContexts, id);

            if (isExisting) {
                chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                    id,
                    changes: rest,
                });
            } else {
                chatbotAttachedContextAdapter.addOne(state.attachedContexts, action.payload);
            }
        },
        attachedContextIncluded: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.updateOne(state.attachedContexts, {
                id: action.payload,
                changes: { state: "included" },
            });
        },
        attachedContextRemoved: (state, action: PayloadAction<ChatbotAttachedContextId>) => {
            chatbotAttachedContextAdapter.removeOne(state.attachedContexts, action.payload);
        },
        attachedContextUnrelatedRemoved: (state) => {
            const typesToKeep: ChatbotAttachedContext["type"][] = ["View", "DatabaseName"];

            chatbotAttachedContextAdapter.removeMany(
                state.attachedContexts,
                chatbotAttachedContextSelectors
                    .selectAll(state.attachedContexts)
                    .filter((x) => !typesToKeep.includes(x.type))
                    .map((x) => x.id)
            );
        },
        attachedContextTypesRemoved: (state, action: PayloadAction<ChatbotAttachedContext["type"][]>) => {
            chatbotAttachedContextAdapter.removeMany(
                state.attachedContexts,
                chatbotAttachedContextSelectors
                    .selectAll(state.attachedContexts)
                    .filter((x) => action.payload.includes(x.type))
                    .map((x) => x.id)
            );
        },
        isNewContextOpenToggled: (state) => {
            const newState = !state.isNewContextOpen;
            state.isNewContextOpen = newState;

            if (!newState) {
                state.newContextTab = null;
            }
        },
        newContextTabSet: (state, action: PayloadAction<ChatbotAttachedContext["type"]>) => {
            state.newContextTab = action.payload;
        },
        deniedEndpointsAdded: (state, action: PayloadAction<string[]>) => {
            state.deniedEndpoints = [...new Set([...state.deniedEndpoints, ...action.payload])];
        },
        isAlwaysAllowEndpointCallsSet: (state, action: PayloadAction<boolean>) => {
            state.isAlwaysAllowEndpointCalls = action.payload;
        },
        isRunQueryFromChatbotSet: (state, action: PayloadAction<boolean>) => {
            state.isRunQueryFromChatbot = action.payload;
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
            endpoints: [],
            userActionState: null,
        };

        dispatch(chatbotActions.messageAdded(assistantMessage));

        const result = await processStreamingResponse<RunChatbotAiAssistantResultDto>({
            promiseFn: () =>
                services.aiAssistantService.runChatbot({
                    Message: payload.message,
                    ConversationId: chatbot.conversationId,
                    ActionsResponses: payload.actionResponses,
                    AdditionalAttachedContext: getAdditionalAttachedContext(attachedContexts),
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

        if (result.status !== "Success") {
            return {
                ...assistantMessage,
                state: result.status,
                errorMessage: result.error,
            };
        }

        dispatch(aiAssistantActions.usagePercentageSet(result.data.UsagePercentage));
        dispatch(chatbotActions.conversationIdSet(result.data.ConversationId));
        dispatch(chatbotActions.attachedContextTypesRemoved(["QueryResult", "QueryError"]));

        const data = result.data;

        return {
            ...assistantMessage,
            state: data.Status,
            content: data.Response.Answer,
            thinkingTimeInMs: new Date().getTime() - startThinkingTime,
            relevantLinks: data.Response.RelevantLinks ?? [],
            followUpQuestions: data.Response.FollowUpQuestions ?? [],
            endpoints: getEndpointItems(data.Endpoints ?? {}),
            userActionState: getUserActionState(data),
        };
    }
);

const exportConversation = createAsyncThunk(chatbotSlice.name + "/exportConversation", async (_, { getState }) => {
    const { chatbot, license } = getState() as RootState;

    fileDownloader.downloadAsJson(
        {
            licenseId: license.status.Id,
            conversationId: chatbot.conversationId,
            exportedAtLocal: moment().format(),
            exportedAtUTC: moment.utc().format(),
            isAlwaysAllowEndpointCalls: chatbot.isAlwaysAllowEndpointCalls,
            deniedEndpoints: chatbot.deniedEndpoints,
            lastRunData: chatbot.lastRunData,
            attachedContexts: chatbotAttachedContextSelectors.selectAll(chatbot.attachedContexts),
            messages: chatbotMessagesSelectors.selectAll(chatbot.messages),
        },
        `conversation-${chatbot.conversationId}.json`
    );
});

function getEndpointItems(endpointsDto: Record<string, string[]>): ChatbotEndpointItem[] {
    if (!_.isObject(endpointsDto)) {
        return [];
    }

    return Object.entries(endpointsDto).flatMap(([toolId, endpoints]) =>
        endpoints.map((url) => ({
            toolId,
            url,
            state: "waiting",
        }))
    );
}

function getAdditionalAttachedContext(attachedContexts: ChatbotAttachedContext[]): Record<string, any> {
    const result: Record<string, any> = Object.fromEntries(
        attachedContexts
            .filter((x) => x.type !== "QueryResult" && x.type !== "QueryError")
            .map((context) => [context.type, context.value])
    );

    const queryResults = attachedContexts.filter((x) => x.type === "QueryResult");
    if (queryResults.length) {
        result["Query Results"] = queryResults.map((x) => ({ query: x.query, result: x.value }));
    }

    const queryErrors = attachedContexts.filter((x) => x.type === "QueryError");
    if (queryErrors.length) {
        result["Query Errors"] = queryErrors.map((x) => ({ query: x.query, error: x.value }));
    }

    return result;
}

function getUserActionState(data: RunChatbotAiAssistantResultDto): ChatbotUserActionState {
    if (!_.isEmpty(data.Endpoints)) {
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
    exportConversation,
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
    attachedContextById: (state: RootState, id: ChatbotAttachedContextId) =>
        chatbotAttachedContextSelectors.selectById(state.chatbot.attachedContexts, id),
    isNewContextOpen: (state: RootState) => state.chatbot.isNewContextOpen,
    newContextTab: (state: RootState) => state.chatbot.newContextTab,
    deniedEndpoints: (state: RootState) => state.chatbot.deniedEndpoints,
    isAlwaysAllowEndpointCalls: (state: RootState) => state.chatbot.isAlwaysAllowEndpointCalls,
    isRunQueryFromChatbot: (state: RootState) => state.chatbot.isRunQueryFromChatbot,
};
