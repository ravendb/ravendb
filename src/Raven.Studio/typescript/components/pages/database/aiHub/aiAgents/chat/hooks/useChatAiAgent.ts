import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import { chatAiAgentActions, chatAiAgentSelectors } from "../store/chatAiAgentSlice";
import { tryHandleSubmit } from "components/utils/common";
import { TimeInSeconds } from "common/constants/timeInSeconds";
import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useChanges } from "components/hooks/useChanges";
import { useServices } from "components/hooks/useServices";
import { useAppDispatch, useAppSelector } from "components/store";
import { useEffect } from "react";
import { useAsyncCallback } from "react-async-hook";
import { useForm } from "react-hook-form";
import { AiAgentToolCall } from "../../utils/aiAgentsTypes";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "../utils/chatAiAgentValidation";
import { useAppUrls } from "components/hooks/useAppUrls";
import router from "plugins/router";

export interface ChatAiAgentQueryParams {
    agentId: string;
    conversationId: string;
    isHistory: boolean;
}

export default function useChatAiAgent(queryParams: ChatAiAgentQueryParams) {
    const dispatch = useAppDispatch();
    const { databasesService } = useServices();
    const { databaseChangesApi } = useChanges();
    const { appUrl } = useAppUrls();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const isDocumentExpirationEnabled = useAppSelector(chatAiAgentSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";
    const document = useAppSelector(chatAiAgentSelectors.document);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);

    // Reset store on unmount
    useEffect(() => {
        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

    const currentDocumentChangeVector = document.data?.["@metadata"]?.["@change-vector"];

    // Watch for document changes
    useEffect(() => {
        let setIsChangedTimeout: NodeJS.Timeout;

        if (databaseChangesApi && conversationId) {
            const watchDocument = databaseChangesApi.watchDocument(conversationId, (e) => {
                if (isLoading || e.ChangeVector === currentDocumentChangeVector) {
                    clearTimeout(setIsChangedTimeout);
                    return;
                }

                if (e.Type === "Delete") {
                    dispatch(chatAiAgentActions.isDocumentDeletedSet(true));
                } else {
                    // onChange callback sends each event separately, so we need to wait a bit before checking
                    // when the last one has the same change vector as the current document this timeout will be cleared
                    setIsChangedTimeout = setTimeout(() => {
                        dispatch(chatAiAgentActions.isDocumentChangedSet(true));
                    }, 100);
                }
            });

            return () => {
                watchDocument.off();
                clearTimeout(setIsChangedTimeout);
            };
        }
    }, [databaseChangesApi, conversationId, currentDocumentChangeVector, isLoading]);

    const asyncGetDefaultValues = useAsyncCallback<ChatAiAgentFormData>(async () => {
        const isDocumentExpirationEnabled = await dispatch(
            chatAiAgentActions.getIsDocumentExpirationEnabled(databaseName)
        ).unwrap();

        dispatch(chatAiAgentActions.conversationIdSet(queryParams?.conversationId));

        const config = await dispatch(
            chatAiAgentActions.getConfig({ databaseName, id: queryParams?.agentId })
        ).unwrap();

        if (queryParams?.conversationId) {
            dispatch(chatAiAgentActions.getDocument({ databaseName, id: queryParams?.conversationId }));
        }

        return {
            prompts: [{ text: "" }],
            attachments: [],
            parameters: config.Parameters.map((x): ChatAiAgentFormData["parameters"][number] => ({
                name: x.Name,
                value: null,
                type: x.Type ?? "Default",
                isSendToModel: x.SendToModel ?? true,
            })),
            isEnableDocumentExpiration: !isDocumentExpirationEnabled,
            isDocumentExpireInCustomizeEnabled: false,
            persistenceConversationIdPrefix: "",
            persistenceExpiresInSeconds: TimeInSeconds.Day * 30,
        } satisfies Required<ChatAiAgentFormData>;
    });

    const areParametersRequired = !window.location.href.includes("conversationId");

    const chatForm = useForm<ChatAiAgentFormData>({
        resolver: chatAiAgentYupResolver,
        defaultValues: asyncGetDefaultValues.execute,
        context: {
            areParametersRequired,
        },
    });

    const reloadForm = async () => {
        const result = await asyncGetDefaultValues.execute();
        chatForm.reset(result);
    };

    const { getValues, handleSubmit, setValue } = chatForm;

    const runChat = async (toolCallParameters?: AiAgentToolCall[]) => {
        const formValues = getValues();

        await dispatch(
            chatAiAgentActions.runChat({
                databaseName,
                formValues,
                toolCallParameters,
                isDocumentExpirationEnabled: isDocumentExpirationEnabled.data,
            })
        ).unwrap();

        setValue("prompts", [{ text: "" }]);
        setValue("attachments", []);
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            const formValues = getValues();

            if (
                queryParams?.conversationId == null &&
                isDocumentExpirationEnabled.status === "success" &&
                !isDocumentExpirationEnabled.data &&
                formValues.isEnableDocumentExpiration
            ) {
                await databasesService.saveExpirationConfiguration(databaseName, {
                    Disabled: false,
                    DeleteFrequencyInSec: isCommunityLicense ? minimumCommunityDeleteFrequencyInSec : null,
                    MaxItemsToProcess: defaultItemsToProcess,
                });
            }

            runChat();
        });
    };

    const handleNewChat = () => {
        router.navigate(appUrl.forChatAiAgent(databaseName, queryParams.agentId));
    };

    return {
        chatForm,
        reloadForm,
        handleSend,
        handleNewChat,
        handleSubmit,
        runChat,
        asyncGetDefaultValues,
    };
}

const minimumCommunityDeleteFrequencyInSec = TimeInSeconds.Day * 36;
