import { createAsyncThunk, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentDocMessage, AiAgentMessage, AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData, loadStatus } from "components/models/common";
import { createSuccessState, createIdleState, createFailureState } from "components/utils/common";
import document from "models/database/documents/document";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import { RunAiAgentRequestDto } from "commands/database/aiAgents/runAiAgentCommand";

interface EditAiAgentState {
    config: loadableData<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;
    document: loadableData<documentDto>;
    runChatState: loadStatus;
    conversationId: string;
    messages: AiAgentMessage[];
    isRawData: boolean;
    isWaitingForActionToolSubmit: boolean;
    hasScroll: boolean;
    isDocumentExpirationEnabled: loadableData<boolean>;
    isDocumentDeleted: boolean;
    isDocumentChanged: boolean;
    activePromptIndex: number;
}

const initialState: EditAiAgentState = {
    config: createIdleState(),
    document: createIdleState(),
    runChatState: "idle",
    conversationId: "",
    messages: [],
    isRawData: false,
    isWaitingForActionToolSubmit: false,
    hasScroll: false,
    isDocumentExpirationEnabled: createIdleState(),
    isDocumentDeleted: false,
    isDocumentChanged: false,
    activePromptIndex: 0,
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
        isWaitingForActionToolSubmitSet: (state, action: PayloadAction<boolean>) => {
            state.isWaitingForActionToolSubmit = action.payload;
        },
        hasScrollSet: (state, action: PayloadAction<boolean>) => {
            state.hasScroll = action.payload;
        },
        isDocumentDeletedSet: (state, action: PayloadAction<boolean>) => {
            state.isDocumentDeleted = action.payload;
        },
        isDocumentChangedSet: (state, action: PayloadAction<boolean>) => {
            state.isDocumentChanged = action.payload;
        },
        activePromptIndexSet: (state, action: PayloadAction<number>) => {
            state.activePromptIndex = action.payload;
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
                state.isDocumentChanged = false;

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
            })
            .addCase(getIsDocumentExpirationEnabled.pending, (state) => {
                state.isDocumentExpirationEnabled.status = "loading";
            })
            .addCase(getIsDocumentExpirationEnabled.rejected, (state, action) => {
                state.isDocumentExpirationEnabled = createFailureState(action.error.message);
            })
            .addCase(getIsDocumentExpirationEnabled.fulfilled, (state, action) => {
                state.isDocumentExpirationEnabled = createSuccessState(action.payload);
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
            return result.toDto(true);
        }
        return result;
    }
);

const runChat = createAsyncThunk(
    chatAiAgentSlice.name + "/runChat",
    async (
        payload: {
            databaseName: string;
            formValues: ChatAiAgentFormData;
            isDocumentExpirationEnabled: boolean;
            toolCallParameters?: AiAgentToolCall[];
        },
        { getState, dispatch }
    ): Promise<void> => {
        const { databaseName, formValues, isDocumentExpirationEnabled, toolCallParameters } = payload;

        const state = getState() as RootState;
        const conversationId = state.chatAiAgent.conversationId;
        const config = state.chatAiAgent.config;
        const changeVector = state.chatAiAgent.document.data?.["@metadata"]?.["@change-vector"] ?? "";

        const result = await services.aiAgentService.runAiAgent(
            databaseName,
            {
                UserPrompt: getUserPrompt(toolCallParameters?.length ?? 0, formValues.prompts),
                ArtificialActions: [],
                ActionResponses: toolCallParameters?.map((x) => ({
                    ToolId: x.id,
                    Content: x.arguments,
                })),
                CreationOptions: {
                    Parameters:
                        conversationId == null
                            ? Object.fromEntries(formValues.parameters.map((x) => [x.name, x.value]))
                            : null,
                    ExpirationInSec:
                        (isDocumentExpirationEnabled || formValues.isEnableDocumentExpiration) &&
                        formValues.isDocumentExpireInCustomizeEnabled
                            ? formValues.persistenceExpiresInSeconds
                            : null,
                },
            },
            config.data?.Identifier,
            conversationId != null ? conversationId : formValues.persistenceConversationIdPrefix,
            changeVector
        );
        dispatch(chatAiAgentActions.activePromptIndexSet(0));
        dispatch(chatAiAgentActions.conversationIdSet(result.ConversationId));
        await dispatch(chatAiAgentActions.getDocument({ databaseName, id: result.ConversationId })).unwrap();
    }
);

function getUserPrompt(
    toolCallParametersCount: number,
    prompts: ChatAiAgentFormData["prompts"]
): RunAiAgentRequestDto["UserPrompt"] {
    if (toolCallParametersCount > 0) {
        return null;
    }

    if (!prompts?.length) {
        throw new Error("Prompt is required");
    }

    if (prompts.length > 1) {
        return prompts.map((x) => ({ type: "text", text: x.text }));
    }

    return prompts[0].text;
}

const getIsDocumentExpirationEnabled = createAsyncThunk(
    chatAiAgentSlice.name + "/getIsDocumentExpirationEnabled",
    async (databaseName: string): Promise<boolean> => {
        const result = await services.databasesService.getExpirationConfiguration(databaseName);
        if (!result) {
            return false;
        }
        return !result.Disabled;
    }
);

export const chatAiAgentActions = {
    ...chatAiAgentSlice.actions,
    getConfig,
    getDocument,
    runChat,
    getIsDocumentExpirationEnabled,
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
    isWaitingForActionToolSubmit: (state: RootState) => state.chatAiAgent.isWaitingForActionToolSubmit,
    hasScroll: (state: RootState) => state.chatAiAgent.hasScroll,
    isDocumentExpirationEnabled: (state: RootState) => state.chatAiAgent.isDocumentExpirationEnabled,
    isDocumentDeleted: (state: RootState) => state.chatAiAgent.isDocumentDeleted,
    isDocumentChanged: (state: RootState) => state.chatAiAgent.isDocumentChanged,
    activePromptIndex: (state: RootState) => state.chatAiAgent.activePromptIndex,
};
