import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentDocMessage, AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData, loadStatus } from "components/models/common";
import { createSuccessState, createIdleState, createFailureState } from "components/utils/common";
import document from "models/database/documents/document";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";

interface EditAiAgentState {
    config: loadableData<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;
    document: loadableData<documentDto>;
    runChatState: loadStatus;
    conversationId: string;
    messages: AiAgentMessage[];
    isRawData: boolean;
}

const initialState: EditAiAgentState = {
    config: createIdleState(),
    document: createIdleState(),
    runChatState: "idle",
    conversationId: "",
    messages: [],
    isRawData: false,
};

export const chatAiAgentSlice = createSlice({
    name: "chatAiAgent",
    initialState,
    reducers: {
        conversationIdSet: (state, action: PayloadAction<string>) => {
            state.conversationId = action.payload;
        },
        messagesSet: (state, action: PayloadAction<AiAgentMessage[]>) => {
            state.messages = action.payload;
        },
        documentSet: (state, action: PayloadAction<documentDto>) => {
            state.document = createSuccessState(action.payload);
        },
        isRawDataSet: (state, action: PayloadAction<boolean>) => {
            state.isRawData = action.payload;
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
            .addCase(getDocument.pending, (state) => {
                state.document.status = "loading";
            })
            .addCase(getDocument.rejected, (state, action) => {
                state.document = createFailureState(action.error.message);
            })
            .addCase(getDocument.fulfilled, (state, action) => {
                state.document = createSuccessState(action.payload);

                const messages: AiAgentMessage[] = action.payload.Messages.map((x: AiAgentDocMessage) =>
                    aiAgentsUtils.mapMessageFromDoc(x)
                );

                state.messages = aiAgentsUtils.mergeToolResults(
                    messages,
                    state.config.data?.Queries.map((x) => x.Name) ?? []
                );
            })
            .addCase(runChat.pending, (state) => {
                state.runChatState = "loading";
            })
            .addCase(runChat.rejected, (state) => {
                state.runChatState = "failure";
            })
            .addCase(runChat.fulfilled, (state) => {
                state.runChatState = "success";
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
        return agents.AiAgents[0];
    }
);

const getDocument = createAsyncThunk(
    chatAiAgentSlice.name + "/getDocument",
    async (payload: { databaseName: string; id: string }): Promise<documentDto> => {
        const result = await services.databasesService.getDocumentWithMetadata(payload.id, payload.databaseName);

        if (result instanceof document) {
            return result.toDto();
        }
        return result;
    }
);

const runChat = createAsyncThunk(
    chatAiAgentSlice.name + "/runChat",
    async (
        payload: {
            databaseName: string;
            prompt: string;
            initialParameters: { name?: string; value?: string }[];
            toolCallParameters?: AiAgentToolCall[];
        },
        { getState, dispatch }
    ): Promise<void> => {
        const { databaseName, prompt, initialParameters, toolCallParameters } = payload;

        const state = getState() as RootState;
        const conversationId = state.chatAiAgent.conversationId;
        const config = state.chatAiAgent.config;

        const result = await services.aiAgentService.runAiAgent(
            databaseName,
            {
                UserPrompt: toolCallParameters?.length > 0 ? null : prompt,
                Parameters:
                    conversationId == null ? Object.fromEntries(initialParameters.map((x) => [x.name, x.value])) : null,
                ActionResponses: toolCallParameters?.map((x) => ({
                    ToolId: x.id,
                    Content: x.arguments,
                })),
            },
            conversationId != null ? undefined : config.data?.Identifier,
            conversationId != null ? conversationId : undefined
        );
        dispatch(chatAiAgentActions.conversationIdSet(result.ConversationId));
        await dispatch(chatAiAgentActions.getDocument({ databaseName, id: result.ConversationId })).unwrap();
    }
);

export const chatAiAgentActions = {
    ...chatAiAgentSlice.actions,
    getConfig,
    getDocument,
    runChat,
};

export const chatAiAgentSelectors = {
    messages: (state: RootState) => state.chatAiAgent.messages,
    conversationId: (state: RootState) => state.chatAiAgent.conversationId,
    config: (state: RootState) => state.chatAiAgent.config,
    isRawData: (state: RootState) => state.chatAiAgent.isRawData,
    document: (state: RootState) => state.chatAiAgent.document,
    runChatState: (state: RootState) => state.chatAiAgent.runChatState,
    isLoading: (state: RootState) =>
        state.chatAiAgent.runChatState === "loading" ||
        state.chatAiAgent.config.status === "loading" ||
        state.chatAiAgent.document.status === "loading",
};
