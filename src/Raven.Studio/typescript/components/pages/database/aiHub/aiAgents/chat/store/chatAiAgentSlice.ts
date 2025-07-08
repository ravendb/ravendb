import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentMessage } from "../../partials/AiAgentMessages";
import { services } from "components/hooks/useServices";
import { loadableData } from "components/models/common";
import { createSuccessState, createIdleState, createFailureState } from "components/utils/common";

interface DocMessage {
    role: string;
    content: string;
}

interface EditAiAgentState {
    config: loadableData<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;
    historyDocuments: loadableData<documentDto[]>;
    prompt: string;
    chatId: string;
    messages: AiAgentMessage[];
}

const initialState: EditAiAgentState = {
    config: createIdleState(),
    historyDocuments: createIdleState([]),
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
        messagesSet: (state, action: PayloadAction<AiAgentMessage[]>) => {
            state.messages = action.payload;
        },
        historyChatSelected: (state, action: PayloadAction<{ docId: string }>) => {
            const docId = action.payload.docId;
            state.chatId = docId;

            const messagesFromDoc: DocMessage[] =
                state.historyDocuments.data.find((d) => d["@metadata"]["@id"] === docId)?.Messages ?? [];

            const getRole = (docMessage: DocMessage): AiAgentMessage["author"] => {
                if (docMessage.role === "user") {
                    return "user";
                }
                if (docMessage.role === "assistant") {
                    return "agent";
                }

                return "agent";
            };

            const getText = (docMessage: DocMessage): string => {
                if (docMessage.role === "user") {
                    return docMessage.content;
                }
                return JSON.stringify(JSON.parse(docMessage.content), null, 2);
            };

            const messages = messagesFromDoc
                .filter((x) => x.role === "assistant" || x.role === "user")
                .map(
                    (x) =>
                        ({
                            id: _.uniqueId(),
                            author: getRole(x),
                            text: getText(x),
                            state: "success",
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
            });
    },
});

const getConfig = createAsyncThunk(
    chatAiAgentSlice.name + "/getConfig",
    async (payload: {
        databaseName: string;
        agentName: string;
    }): Promise<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration> => {
        return services.aiAgentService.getAiAgents(payload.databaseName, payload.agentName);
    }
);

const getHistoryDocuments = createAsyncThunk(
    chatAiAgentSlice.name + "/getHistoryDocuments",
    async (payload: { databaseName: string; agentName: string }): Promise<documentDto[]> => {
        return services.databasesService.getDocumentsByIDPrefix(payload.agentName, 1024, payload.databaseName);
    }
);

export const chatAiAgentActions = {
    ...chatAiAgentSlice.actions,
    getConfig,
    getHistoryDocuments,
};

export const chatAiAgentSelectors = {
    prompt: (state: RootState) => state.chatAiAgent.prompt,
    messages: (state: RootState) => state.chatAiAgent.messages,
    chatId: (state: RootState) => state.chatAiAgent.chatId,
    historyDocuments: (state: RootState) => state.chatAiAgent.historyDocuments,
};
