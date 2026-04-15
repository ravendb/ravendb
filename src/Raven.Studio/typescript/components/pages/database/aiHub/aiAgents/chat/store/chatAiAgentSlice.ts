import { createAsyncThunk, createSelector, createSlice, PayloadAction } from "@reduxjs/toolkit";
import { RootState } from "components/store";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { services } from "components/hooks/useServices";
import { loadableData, loadStatus } from "components/models/common";
import { createSuccessState, createIdleState, createFailureState } from "components/utils/common";
import document from "models/database/documents/document";
import { aiAgentsUtils } from "../../utils/aiAgentsUtils";
import { aiAgentParametersUtils } from "../../utils/aiAgentParametersUtils";
import { ChatAiAgentFormData } from "../utils/chatAiAgentValidation";
import { RunAiAgentRequestDto } from "commands/database/aiAgents/runAiAgentCommand";

type NewAttachmentTab = { tab: "source" } | { tab: "document" } | { tab: "documentAttachments"; documentId: string };

interface ChatAiAgentState {
    config: loadableData<Raven.Client.Documents.Operations.AI.Agents.AiAgentConfiguration>;
    document: loadableData<documentDto>;
    runChatState: loadStatus;
    conversationId: string;
    isRawData: boolean;
    isDocumentExpirationEnabled: loadableData<boolean>;
    isDocumentDeleted: boolean;
    isDocumentChanged: boolean;
    activePromptIndex: number;
    newAttachmentTab: NewAttachmentTab;
}

const initialState: ChatAiAgentState = {
    config: createIdleState(),
    document: createIdleState(),
    runChatState: "idle",
    conversationId: "",
    isRawData: false,
    isDocumentExpirationEnabled: createIdleState(),
    isDocumentDeleted: false,
    isDocumentChanged: false,
    activePromptIndex: 0,
    newAttachmentTab: null,
};

export const chatAiAgentSlice = createSlice({
    name: "chatAiAgent",
    initialState,
    reducers: {
        conversationIdSet: (state, action: PayloadAction<string>) => {
            state.conversationId = action.payload;
        },
        documentSet: (state, action: PayloadAction<documentDto>) => {
            state.document = createSuccessState(action.payload);
        },
        isRawDataSet: (state, action: PayloadAction<boolean>) => {
            state.isRawData = action.payload;
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
        newAttachmentTabSet: (state, action: PayloadAction<NewAttachmentTab>) => {
            state.newAttachmentTab = action.payload;
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
                UserPrompt: createUserPromptDto(toolCallParameters?.length ?? 0, formValues.prompts),
                ArtificialActions: [],
                ActionResponses: toolCallParameters?.map((x) => ({
                    ToolId: x.id,
                    Content: x.arguments,
                })),
                AttachmentCommands: null,
                attachments: formValues.attachments,
                CreationOptions: {
                    Parameters: createParametersDto(conversationId, formValues.parameters),
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

function createUserPromptDto(
    toolCallParametersCount: number,
    prompts: ChatAiAgentFormData["prompts"]
): RunAiAgentRequestDto["UserPrompt"] {
    if (toolCallParametersCount > 0) {
        return null;
    }

    const validPrompts = prompts?.filter((x) => x?.text?.trim()) ?? [];

    if (validPrompts.length > 1) {
        return validPrompts.map((x) => ({ type: "text", text: x.text.trim() }));
    }

    return prompts[0].text;
}

function createParametersDto(
    conversationId: string,
    formParameters: ChatAiAgentFormData["parameters"]
): Record<string, Raven.Client.Documents.AI.AiConversationParameter> {
    if (conversationId) {
        return null;
    }

    return Object.fromEntries(
        formParameters.map((x) => [
            x.name,
            {
                Value: aiAgentParametersUtils.mapParameterValueToType(x.value, x.type),
                SendToModel: x.isSendToModel,
            },
        ])
    );
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

const selectChatAiAgentConfig = (state: RootState) => state.chatAiAgent.config.data;
const selectChatAiAgentDocument = (state: RootState) => state.chatAiAgent.document.data;

const selectChatAiAgentMessages = createSelector(
    [selectChatAiAgentDocument, selectChatAiAgentConfig],
    (conversationDocument, config) =>
        aiAgentsUtils.mapMessagesFromDoc({
            conversationDocument,
            config,
        })
);

export const chatAiAgentSelectors = {
    messages: selectChatAiAgentMessages,
    conversationId: (state: RootState) => state.chatAiAgent.conversationId,
    config: (state: RootState) => state.chatAiAgent.config,
    isRawData: (state: RootState) => state.chatAiAgent.isRawData,
    document: (state: RootState) => state.chatAiAgent.document,
    documentAttachments: (state: RootState) => state.chatAiAgent.document.data?.["@metadata"]?.["@attachments"] ?? [],
    runChatState: (state: RootState) => state.chatAiAgent.runChatState,
    isLoading: (state: RootState) =>
        state.chatAiAgent.runChatState === "loading" ||
        state.chatAiAgent.config.status === "loading" ||
        state.chatAiAgent.document.status === "loading",
    isActionToolSubmitRequired: createSelector([selectChatAiAgentDocument], (conversationDocument) =>
        aiAgentsUtils.hasOpenActionCalls(conversationDocument)
    ),
    isDocumentExpirationEnabled: (state: RootState) => state.chatAiAgent.isDocumentExpirationEnabled,
    isDocumentDeleted: (state: RootState) => state.chatAiAgent.isDocumentDeleted,
    isDocumentChanged: (state: RootState) => state.chatAiAgent.isDocumentChanged,
    activePromptIndex: (state: RootState) => state.chatAiAgent.activePromptIndex,
    newAttachmentTab: (state: RootState) => state.chatAiAgent.newAttachmentTab,
};
