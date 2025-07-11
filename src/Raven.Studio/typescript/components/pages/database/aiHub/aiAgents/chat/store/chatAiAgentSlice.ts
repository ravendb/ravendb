import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData } from "components/models/common";
import { createSuccessState, createIdleState, createFailureState } from "components/utils/common";
import document from "models/database/documents/document";

interface DocMessage {
    role: "user" | "assistant" | "system" | "tool";
    content: string;
    tool_calls: {
        id: string;
        type: string;
        function: {
            name: string;
            arguments: string;
        };
    }[];
}

interface EditAiAgentState {
    config: loadableData<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;
    historyDocuments: loadableData<documentDto[]>;
    currentDocument: loadableData<documentDto>;
    conversationId: string;
    messages: AiAgentMessage[];
    toolParameters: AiAgentToolCall[];
}

const initialState: EditAiAgentState = {
    config: createIdleState(),
    historyDocuments: createIdleState([]),
    currentDocument: createIdleState(),
    conversationId: "",
    messages: [],
    toolParameters: [],
};

export const chatAiAgentSlice = createSlice({
    name: "chatAiAgent",
    initialState,
    reducers: {
        conversationIdSet: (state, action: PayloadAction<string>) => {
            state.conversationId = action.payload;
        },
        messagesAdd: (state, action: PayloadAction<AiAgentMessage>) => {
            state.messages.push(action.payload);
        },
        messagesUpdate: (
            state,
            action: PayloadAction<Pick<AiAgentMessage, "id" | "state" | "content" | "usage" | "toolCalls">>
        ) => {
            const message = state.messages.find((m) => m.id === action.payload.id);
            if (message) {
                Object.assign(message, action.payload);
            }
        },
        messagesSet: (state, action: PayloadAction<AiAgentMessage[]>) => {
            state.messages = action.payload;
        },
        toolParametersSet: (state, action: PayloadAction<AiAgentToolCall[]>) => {
            state.toolParameters = action.payload;
        },
        historyChatSelected: (state, action: PayloadAction<{ docId: string }>) => {
            const docId = action.payload.docId;
            state.conversationId = docId;

            const messagesFromDoc: DocMessage[] =
                state.historyDocuments.data.find((x) => x["@metadata"]["@id"] === docId)?.Messages ?? [];

            const getContent = (docMessage: DocMessage): string => {
                if (docMessage.content && (docMessage.role === "assistant" || docMessage.role === "tool")) {
                    return JSON.stringify(JSON.parse(docMessage.content), null, 2);
                }
                return docMessage.content;
            };

            const messages = messagesFromDoc.map(
                (x) =>
                    ({
                        id: _.uniqueId(),
                        role: x.role,
                        content: getContent(x),
                        state: "success",
                        date: "TODO date",
                        toolCalls: x.tool_calls
                            ? x.tool_calls.map((x) => ({
                                  id: x.id,
                                  name: x.function.name,
                                  arguments: x.function.arguments,
                              }))
                            : [],
                    }) satisfies AiAgentMessage
            );

            state.messages = messages;
        },
        reset: () => initialState,
    },
    extraReducers: (builder) => {
        builder
            .addCase(getConfig.pending, (state) => {
                state.config.status = "loading";
            })
            .addCase(getConfig.rejected, (state, action) => {
                state.config = createFailureState(action.error.message);
            })
            .addCase(getConfig.fulfilled, (state, action) => {
                state.config = createSuccessState(action.payload);
            })
            .addCase(getHistoryDocuments.pending, (state) => {
                state.historyDocuments.status = "loading";
            })
            .addCase(getHistoryDocuments.rejected, (state, action) => {
                state.historyDocuments = createFailureState(action.error.message);
            })
            .addCase(getHistoryDocuments.fulfilled, (state, action) => {
                state.historyDocuments = createSuccessState(action.payload);
            })
            .addCase(getCurrentDocument.pending, (state) => {
                state.currentDocument.status = "loading";
            })
            .addCase(getCurrentDocument.rejected, (state, action) => {
                state.currentDocument = createFailureState(action.error.message);
            })
            .addCase(getCurrentDocument.fulfilled, (state, action) => {
                state.currentDocument = createSuccessState(action.payload);
            });
    },
});

const getConfig = createAsyncThunk(
    chatAiAgentSlice.name + "/getConfig",
    async (payload: {
        databaseName: string;
        id: string;
    }): Promise<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration> => {
        const agents = await services.aiAgentService.getAiAgents(payload.databaseName, payload.id);
        return agents[0];
    }
);

const getHistoryDocuments = createAsyncThunk(
    chatAiAgentSlice.name + "/getHistoryDocuments",
    async (payload: { databaseName: string; id: string }): Promise<documentDto[]> => {
        return services.databasesService.getDocumentsByIDPrefix(payload.id, 1024, payload.databaseName);
    }
);

const getCurrentDocument = createAsyncThunk(
    chatAiAgentSlice.name + "/getCurrentDocument",
    async (payload: { databaseName: string; chatId: string }): Promise<documentDto> => {
        const result = await services.databasesService.getDocumentWithMetadata(payload.chatId, payload.databaseName);

        if (result instanceof document) {
            return result.toDto();
        }
        return result;
    }
);

export const chatAiAgentActions = {
    ...chatAiAgentSlice.actions,
    getConfig,
    getHistoryDocuments,
};

export const chatAiAgentSelectors = {
    messages: (state: RootState) => state.chatAiAgent.messages,
    conversationId: (state: RootState) => state.chatAiAgent.conversationId,
    historyDocuments: (state: RootState) => state.chatAiAgent.historyDocuments,
    config: (state: RootState) => state.chatAiAgent.config,
    toolParameters: (state: RootState) => state.chatAiAgent.toolParameters,
};
