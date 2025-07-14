import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { useEffect, useRef } from "react";
import AiAgentMessages from "../partials/AiAgentMessages";
import { Icon } from "components/common/Icon";
import ChatAiAgentInfoHub from "./ChatAiAgentInfoHub";
import Button from "react-bootstrap/Button";
import { useForm, useWatch } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./utils/chatAiAgentValidation";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import { FormInput } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import moment from "moment";
import { aiAgentsUtils } from "../utils/aiAgentsUtils";
import { AiAgentToolCall } from "../utils/aiAgentsTypes";
import SizeGetter from "components/common/SizeGetter";
import classNames from "classnames";
import AceEditor from "components/common/ace/AceEditor";
import { Switch } from "components/common/Checkbox";

interface QueryParams {
    id: string;
}

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const id = queryParams?.id;

    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();
    const { aiAgentService, databasesService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const conversationId = useAppSelector(chatAiAgentSelectors.conversationId);
    const historyDocuments = useAppSelector(chatAiAgentSelectors.historyDocuments);
    const config = useAppSelector(chatAiAgentSelectors.config);
    const isRawData = useAppSelector(chatAiAgentSelectors.isRawData);

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    const { control, handleSubmit, setValue } = useForm<ChatAiAgentFormData>({
        resolver: chatAiAgentYupResolver,
        defaultValues: {
            prompt: "",
            parameters: [],
        },
    });

    const formValues = useWatch({
        control,
    });

    const asyncChat = useAsyncCallback(async (toolParameters?: AiAgentToolCall[]) => {
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: _.uniqueId(),
                content: formValues.prompt,
                role: "user",
                state: "success",
                date: moment().format(aiAgentsUtils.messageDateFormat),
                toolCalls: toolParameters,
            })
        );

        const agentMessageId = _.uniqueId();
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: agentMessageId,
                role: "assistant",
                date: moment().format(aiAgentsUtils.messageDateFormat),
                state: "loading",
            })
        );

        try {
            const result = await aiAgentService.runAiAgent(
                databaseName,
                {
                    UserPrompt: toolParameters?.length > 0 ? null : formValues.prompt,
                    Parameters: !conversationId
                        ? Object.fromEntries(formValues.parameters.map((x) => [x.name, x.value]))
                        : null,
                    ActionResponses: toolParameters?.map((x) => ({
                        ToolId: x.id,
                        Content: x.arguments,
                    })),
                },
                conversationId ? undefined : config.data.Identifier,
                conversationId ? conversationId : undefined
            );

            const doc = await databasesService.getDocumentWithMetadata(result.ConversationId, databaseName);

            dispatch(chatAiAgentActions.conversationIdSet(result.ConversationId));
            dispatch(chatAiAgentActions.toolParametersSet([]));
            setValue("prompt", "");
            dispatch(
                chatAiAgentActions.messagesUpdate(aiAgentsUtils.mapMessageFromResponse(result, agentMessageId, doc))
            );
            dispatch(chatAiAgentActions.getHistoryDocuments({ databaseName, id }));
        } catch {
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    state: "error",
                })
            );
        }
    });

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            return asyncChat.execute();
        });
    };

    // Get data on load
    useEffect(() => {
        dispatch(chatAiAgentActions.getHistoryDocuments({ databaseName, id }));
        dispatch(chatAiAgentActions.getConfig({ databaseName, id })).then((action: TODO) => {
            setValue(
                "parameters",
                action.payload.Parameters.map((x: string) => ({ name: x, value: "" }))
            );
        });

        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

    // Scroll to the bottom of the test panel when new messages are added
    useEffect(() => {
        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    const handleAddChat = () => {
        dispatch(chatAiAgentActions.messagesSet([]));
        dispatch(chatAiAgentActions.conversationIdSet(null));
    };

    if (!id) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
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
                    {messages.length > 0 && (
                        <Button variant="primary" className="rounded-pill" onClick={handleAddChat}>
                            <Icon icon="plus" /> New chat
                        </Button>
                    )}
                    <a className="btn btn-secondary rounded-pill" href={appUrl.forAiAgents(databaseName)}>
                        <Icon icon="cancel" /> Cancel
                    </a>
                </div>
                <Switch
                    color="primary"
                    selected={isRawData}
                    toggleSelection={() => dispatch(chatAiAgentActions.isRawDataSet(!isRawData))}
                >
                    Raw data
                </Switch>
            </div>

            <div className="flex-grow-1">
                <SizeGetter
                    isHeighRequired
                    render={({ height }) => (
                        <div style={{ height }} className="hstack">
                            <div
                                style={{ width: "250px" }}
                                className="p-2 border border-secondary panel-bg-2 h-100 rounded-2"
                            >
                                <h5 className="text-muted">Chat history</h5>
                                {historyDocuments.status === "success" && (
                                    <div className="vstack gap-2">
                                        {historyDocuments.data.map((doc) => (
                                            <div
                                                key={doc["@metadata"]["@id"]}
                                                onClick={() =>
                                                    dispatch(
                                                        chatAiAgentActions.historyChatSelected({
                                                            docId: doc["@metadata"]["@id"],
                                                        })
                                                    )
                                                }
                                                className={classNames(
                                                    "hover-filter cursor-pointer text-truncate p-1 rounded-2",
                                                    {
                                                        "panel-bg-3": conversationId === doc["@metadata"]["@id"],
                                                    }
                                                )}
                                            >
                                                {
                                                    doc.Messages?.find((x: { role: string }) => x.role === "user")
                                                        ?.content
                                                }
                                            </div>
                                        ))}
                                    </div>
                                )}
                            </div>
                            <form className="vstack overflow-auto h-100" onSubmit={handleSubmit(handleSend)}>
                                <div ref={messagesPanelRef} className="overflow-auto ps-2 flex-grow-1">
                                    {messages.length === 0 && (
                                        <div className="p-5">
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
                                            handleSaveParameters={(parameters) => asyncChat.execute(parameters)}
                                        />
                                    )}
                                    {isRawData && messages.length > 0 && (
                                        <AceEditor
                                            mode="json"
                                            value={JSON.stringify(
                                                historyDocuments.data?.find(
                                                    (x) => x["@metadata"]["@id"] === conversationId
                                                ),
                                                null,
                                                2
                                            )}
                                            height={`${height - 100}px`}
                                            readOnly
                                        />
                                    )}
                                </div>
                                <div className="mt-3 px-2">
                                    <div className="position-relative">
                                        <FormInput
                                            type="textarea"
                                            as="textarea"
                                            control={control}
                                            name="prompt"
                                            placeholder="Message an agent"
                                            rows={3}
                                            className="rounded-2"
                                            style={{ resize: "none" }}
                                            onKeyDown={(e) => {
                                                if (e.key === "Enter" && !e.shiftKey) {
                                                    e.preventDefault();
                                                    handleSubmit(handleSend)();
                                                }
                                            }}
                                        />
                                        {formValues.prompt && (
                                            <ButtonWithSpinner
                                                type="submit"
                                                variant="secondary"
                                                icon="arrow-up"
                                                isSpinning={asyncChat.loading}
                                                className="position-absolute rounded-pill"
                                                style={{ right: "10px", bottom: "10px", zIndex: 5 }}
                                            />
                                        )}
                                    </div>
                                </div>
                            </form>
                        </div>
                    )}
                />
            </div>
        </div>
    );
}
