import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import { useEffect } from "react";
import { Icon } from "components/common/Icon";
import ChatAiAgentInfoHub from "./partials/ChatAiAgentInfoHub";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./utils/chatAiAgentValidation";
import { tryHandleSubmit } from "components/utils/common";
import { AiAgentToolCall } from "../utils/aiAgentsTypes";
import SizeGetter from "components/common/SizeGetter";
import { Switch } from "components/common/Checkbox";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { useAsyncCallback } from "react-async-hook";
import { TimeInSeconds } from "common/constants/timeInSeconds";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { useServices } from "components/hooks/useServices";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";
import ChatAiAgentFormBody from "./partials/ChatAiAgentFormBody";

interface QueryParams {
    agentId: string;
    conversationId: string;
    isHistory: boolean;
}

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();
    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const isDocumentExpirationEnabled = useAppSelector(chatAiAgentSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";

    // Reset store on unmount
    useEffect(() => {
        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

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
            prompt: "",
            parameters: config.Parameters.map((x) => ({ name: x.Name, value: "" })),
            isEnableDocumentExpiration: !isDocumentExpirationEnabled,
            isDocumentExpireInCustomizeEnabled: false,
            persistenceConversationIdPrefix: "",
            persistenceExpiresInSeconds: TimeInSeconds.Day * 30,
        };
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

    const { control, handleSubmit, setValue } = chatForm;

    const formValues = useWatch({
        control,
    });

    const runChat = async (toolCallParameters?: AiAgentToolCall[]) => {
        await dispatch(
            chatAiAgentActions.runChat({
                databaseName,
                formValues,
                toolCallParameters,
                isDocumentExpirationEnabled: isDocumentExpirationEnabled.data,
            })
        ).unwrap();

        setValue("prompt", "");
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
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
        dispatch(chatAiAgentActions.conversationIdSet(null));
        dispatch(chatAiAgentActions.messagesSet([]));
        dispatch(chatAiAgentActions.documentSet(null));
        dispatch(chatAiAgentActions.isWaitingForActionToolSubmitSet(false));
        setValue("prompt", "");
    };

    if (!queryParams?.agentId) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
    }

    if (asyncGetDefaultValues.loading) {
        return <LoadingView />;
    }

    if (asyncGetDefaultValues.error) {
        return <LoadError error="Unable to load configuration" refresh={reloadForm} />;
    }

    return (
        <div className="h-100 vstack">
            <div className="hstack justify-content-between align-items-start px-3 pt-3">
                <h2 className="text-truncate w-50 mb-3" title={config.data?.Name}>
                    <Icon icon="ai-agents" /> {config.data?.Name ?? "AI Agent"}{" "}
                </h2>
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack mb-2 justify-content-between px-3">
                <div className="hstack gap-2">
                    <Button
                        variant="primary"
                        className="rounded-pill"
                        onClick={handleNewChat}
                        title="Click to start a new chat with the LLM using this agent"
                    >
                        <Icon icon="plus" /> New chat
                    </Button>
                    <a className="btn btn-secondary rounded-pill" href={appUrl.forAiAgents(databaseName)}>
                        <Icon icon="cancel" /> Cancel
                    </a>
                </div>
                <Switch
                    color="primary"
                    selected={isRawData}
                    toggleSelection={() => dispatch(chatAiAgentActions.isRawDataSet(!isRawData))}
                    title="Toggle on to view the chat communication in raw data format"
                >
                    Raw data
                </Switch>
            </div>

            <div
                className={classNames("flex-grow-1 hstack justify-content-center", { "pb-3": queryParams?.isHistory })}
            >
                <SizeGetter
                    isHeighRequired
                    render={({ height }) => (
                        <FormProvider {...chatForm}>
                            <form
                                className="vstack overflow-auto"
                                onSubmit={handleSubmit(handleSend)}
                                style={{ height }}
                            >
                                <ChatAiAgentFormBody
                                    height={height}
                                    handleSend={handleSend}
                                    runChat={runChat}
                                    isHistory={queryParams?.isHistory}
                                />
                            </form>
                        </FormProvider>
                    )}
                />
            </div>
        </div>
    );
}

const minimumCommunityDeleteFrequencyInSec = TimeInSeconds.Day * 36;
