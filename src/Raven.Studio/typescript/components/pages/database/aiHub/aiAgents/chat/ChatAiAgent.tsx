import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useEffect, useRef } from "react";
import AiAgentMessages from "../partials/AiAgentMessages";
import { Icon } from "components/common/Icon";
import ChatAiAgentInfoHub from "./partials/ChatAiAgentInfoHub";
import { FormProvider, useForm, useWatch } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./utils/chatAiAgentValidation";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import { FormInput } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import { AiAgentToolCall } from "../utils/aiAgentsTypes";
import SizeGetter from "components/common/SizeGetter";
import AceEditor from "components/common/ace/AceEditor";
import { Switch } from "components/common/Checkbox";
import Spinner from "react-bootstrap/Spinner";
import Button from "react-bootstrap/Button";
import classNames from "classnames";
import { useAsyncCallback } from "react-async-hook";
import { TimeInSeconds } from "common/constants/timeInSeconds";
import ChatAiAgentPersistenceSection from "./partials/ChatAiAgentPersistenceSection";
import { LoadError } from "components/common/LoadError";
import { LoadingView } from "components/common/LoadingView";
import { useServices } from "components/hooks/useServices";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { defaultItemsToProcess } from "components/pages/database/settings/documentExpiration/DocumentExpiration";

interface QueryParams {
    agentId: string;
    conversationId: string;
}

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const dispatch = useAppDispatch();
    const messagesPanelRef = useRef<HTMLDivElement>(null);
    const { appUrl } = useAppUrls();
    const { databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);
    const document = useAppSelector(chatAiAgentSelectors.document);
    const runChatState = useAppSelector(chatAiAgentSelectors.runChatState);
    const isLoading = useAppSelector(chatAiAgentSelectors.isLoading);
    const isWaitingForActionToolSubmit = useAppSelector(chatAiAgentSelectors.isWaitingForActionToolSubmit);
    const hasScroll = useAppSelector(chatAiAgentSelectors.hasScroll);
    const isDocumentExpirationEnabled = useAppSelector(chatAiAgentSelectors.isDocumentExpirationEnabled);
    const isCommunityLicense = useAppSelector(licenseSelectors.licenseType) === "Community";

    // Reset store on unmount
    useEffect(() => {
        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

    // Scroll to the bottom of the test panel when new messages are added and set hasScroll
    useEffect(() => {
        dispatch(
            chatAiAgentActions.hasScrollSet(
                messagesPanelRef.current?.scrollHeight > messagesPanelRef.current?.clientHeight
            )
        );

        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

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
        <div className="content-padding h-100 vstack">
            <div className="hstack justify-content-between align-items-start">
                <h2 className="text-truncate w-50 mb-3" title={config.data?.Name}>
                    <Icon icon="ai-agents" /> {config.data?.Name ?? "AI Agent"}{" "}
                </h2>
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack mb-2 justify-content-between">
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

            <div className="flex-grow-1 hstack justify-content-center">
                <SizeGetter
                    isHeighRequired
                    render={({ height }) => (
                        <FormProvider {...chatForm}>
                            <form
                                className="vstack overflow-auto"
                                onSubmit={handleSubmit(handleSend)}
                                style={{ height }}
                            >
                                <div
                                    ref={messagesPanelRef}
                                    className={classNames(
                                        "overflow-auto ps-2 flex-grow-1 position-relative d-flex justify-content-center",
                                        { "pe-2": !hasScroll }
                                    )}
                                    style={{ height: height - promptHeightInPx }}
                                >
                                    <div className="w-100" style={{ maxWidth: "800px" }}>
                                        {messages.length === 0 && (
                                            <div className="h-100 vstack justify-content-center">
                                                <ChatAiAgentPersistenceSection />
                                                <AiAgentParametersField
                                                    control={control}
                                                    name="parameters"
                                                    value={formValues.parameters}
                                                />
                                            </div>
                                        )}
                                        {!isRawData && messages.length > 0 && (
                                            <AiAgentMessages
                                                messages={messages}
                                                toolQueries={config.data?.Queries}
                                                toolActions={config.data?.Actions}
                                                handleSaveParameters={runChat}
                                                setIsWaitingForActionToolSubmit={(value: boolean) =>
                                                    dispatch(chatAiAgentActions.isWaitingForActionToolSubmitSet(value))
                                                }
                                            />
                                        )}
                                        {isRawData && document.data && (
                                            <AceEditor
                                                mode="json"
                                                value={JSON.stringify(document.data, null, 2)}
                                                height={`${height - promptHeightInPx}px`}
                                                readOnly
                                            />
                                        )}
                                        {isLoading && (
                                            <div className="position-absolute top-50 start-50 translate-middle">
                                                <Spinner animation="border" />
                                            </div>
                                        )}
                                    </div>
                                </div>
                                <div className="d-flex justify-content-center mt-3 px-2">
                                    <div className="w-100" style={{ maxWidth: "800px" }}>
                                        <div className="position-relative">
                                            <FormInput
                                                type="textarea"
                                                as="textarea"
                                                control={control}
                                                name="prompt"
                                                placeholder="Ask the agent anything"
                                                className="rounded-2"
                                                style={{ resize: "none" }}
                                                onKeyDown={(e) => {
                                                    if (e.key === "Enter" && !e.shiftKey) {
                                                        e.preventDefault();
                                                        handleSubmit(handleSend)();
                                                    }
                                                }}
                                                disabled={isLoading || isWaitingForActionToolSubmit}
                                            />
                                            {formValues.prompt && (
                                                <ButtonWithSpinner
                                                    type="submit"
                                                    variant="secondary"
                                                    icon="arrow-up"
                                                    isSpinning={runChatState === "loading"}
                                                    disabled={isLoading || isWaitingForActionToolSubmit}
                                                    className="position-absolute rounded-pill"
                                                    style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                                                />
                                            )}
                                        </div>
                                    </div>
                                </div>
                            </form>
                        </FormProvider>
                    )}
                />
            </div>
        </div>
    );
}

const promptHeightInPx = 150;
const minimumCommunityDeleteFrequencyInSec = TimeInSeconds.Day * 36;
