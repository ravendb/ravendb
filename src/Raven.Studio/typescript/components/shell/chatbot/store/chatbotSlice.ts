import {
    createAsyncThunk,
    createEntityAdapter,
    createSlice,
    EntityState,
    PayloadAction,
    Update,
} from "@reduxjs/toolkit";
import genUtils from "common/generalUtils";
import {
    getRunChatbotAssistAiAssistantRequestDto,
    RunChatbotAiAssistantResultDto,
    RunChatbotAiAssistantViewData,
} from "commands/aiAssistant/runChatbotAiAssistantCommand";
import fileDownloader from "common/fileDownloader";
import { aiAssistantActions } from "components/common/shell/aiAssistantSlice";
import { services } from "components/hooks/useServices";
import { RootState } from "components/store";
import { processStreamingResponse } from "components/utils/aiAssistStreamingUtils";
import moment from "moment";

let chatAbortController: AbortController = null;

export const chatbotRequestSizeLimitBytes = 32 * 1024;
export const chatbotRequestSizeWarningBytes = 4 * 1024;
export const chatbotServerMetadataOverheadBytes = 644; // Overhead for metadata added in Raven Server (License + CertificateThumbprint)

type ChatbotTab = "aiAssistant" | "resources";
type ChatbotResourcesTab = "helpAndResources" | "joinTheCommunity" | "contactSupport" | "submitFeedback";
export type ChatbotUserActionState = "waiting" | "allowed" | "alwaysAllowed" | "skipped" | "denied" | "error";

interface ChatbotRunChatData {
    message?: string;
    actionResponses?: Record<string, any>;
}

interface ChatbotRunChatInput extends ChatbotRunChatData {
    conversationId?: string;
    attachedContexts: ChatbotAttachedContext[];
    ravenVersion: number;
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
    resultSizeInBytes?: number;
    isRequestTooLarge?: boolean;
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
    isDataSubmissionEnabled: boolean;
    isAsciiAnimationFinished: boolean;
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
    chatbotTab: "aiAssistant",
    chatbotResourcesTab: "helpAndResources",
    conversationId: null,
    messages: chatbotMessagesAdapter.getInitialState(),
    lastRunData: null,
    deniedEndpoints: [],
    attachedContexts: chatbotAttachedContextAdapter.getInitialState(),
    isNewContextOpen: false,
    newContextTab: null,
    isAlwaysAllowEndpointCalls: true,
    isDataSubmissionEnabled: true,
    isAsciiAnimationFinished: false,
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
        asciiAnimationFinished: (state) => {
            state.isAsciiAnimationFinished = true;
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
        messageRemoved: (state, action: PayloadAction<string>) => {
            chatbotMessagesAdapter.removeOne(state.messages, action.payload);
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
            state.isNewContextOpen = !state.isNewContextOpen;
        },
        isNewContextOpenSet: (state, action: PayloadAction<boolean>) => {
            state.isNewContextOpen = action.payload;
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
        isDataSubmissionEnabledSet: (state, action: PayloadAction<boolean>) => {
            state.isDataSubmissionEnabled = action.payload;
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
    async (payload: ChatbotRunChatData, { dispatch, getState }): Promise<ChatbotAssistantMessage> => {
        const { aiAssistant, chatbot, cluster } = getState() as RootState;

        chatAbortController = new AbortController();

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

        const runChatbotViewData = createChatbotViewData({
            ravenVersion: cluster.serverVersion?.BuildVersion,
            message: payload.message,
            conversationId: chatbot.conversationId,
            actionResponses: payload.actionResponses,
            attachedContexts,
        });

        const result = await processStreamingResponse<RunChatbotAiAssistantResultDto>({
            promiseFn: () => services.aiAssistantService.runChatbot(runChatbotViewData, chatAbortController.signal),
            streamPropertyPath: "Response.Answer",
            onChunksCombined(text) {
                dispatch(
                    chatbotActions.messageUpdated({
                        id: responseId,
                        changes: { state: "Success", content: text },
                    })
                );
            },
            abortSignal: chatAbortController.signal,
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

function createChatbotViewData(data: ChatbotRunChatInput): RunChatbotAiAssistantViewData {
    return {
        RavenVersion: data.ravenVersion,
        Message: data.message,
        ConversationId: data.conversationId,
        ActionsResponses: data.actionResponses,
        AdditionalAttachedContext: getAdditionalAttachedContext(data.attachedContexts),
    };
}

export function estimateChatbotRunChatRequestSize(input: ChatbotRunChatInput): number {
    const viewData = createChatbotViewData(input);
    const requestBody = JSON.stringify(getRunChatbotAssistAiAssistantRequestDto(viewData));
    return genUtils.getSizeInBytesAsUTF8(requestBody) + chatbotServerMetadataOverheadBytes;
}

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

const abortChat = createAsyncThunk(chatbotSlice.name + "/abortChat", async () => {
    if (chatAbortController) {
        chatAbortController.abort();
        chatAbortController = null;
    }
});

export const chatbotActions = {
    ...chatbotSlice.actions,
    runChat,
    retryRunChat,
    abortChat,
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
    isDataSubmissionEnabled: (state: RootState) => state.chatbot.isDataSubmissionEnabled,
    isAsciiAnimationFinished: (state: RootState) => state.chatbot.isAsciiAnimationFinished,
};
