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
import { AboutViewHeading } from "components/common/AboutView";
import ChatAiAgentInfoHub from "./ChatAiAgentInfoHub";
import Button from "react-bootstrap/Button";
import { useForm, useWatch } from "react-hook-form";
import { ChatAiAgentFormData, chatAiAgentYupResolver } from "./utils/chatAiAgentValidation";
import AiAgentParametersField from "../partials/AiAgentParametersField";
import { FormInput } from "components/common/Form";
import { tryHandleSubmit } from "components/utils/common";
import moment from "moment";

type ChatResult = Raven.Client.Documents.Operations.AI.Agents.ChatResult<object>;

interface QueryParams {
    agentName: string;
}

export default function ChatAiAgent({ queryParams }: ReactQueryParamsProps<QueryParams>) {
    const agentName = queryParams?.agentName;

    const dispatch = useAppDispatch();
    const { appUrl } = useAppUrls();
    const { aiAgentService } = useServices();

    const databaseName = useAppSelector(databaseSelectors.activeDatabaseName);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const chatId = useAppSelector(chatAiAgentSelectors.chatId);
    const historyDocuments = useAppSelector(chatAiAgentSelectors.historyDocuments);

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

    const asyncChat = useAsyncCallback(async (chatAction: () => Promise<ChatResult>) => {
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: _.uniqueId(),
                content: formValues.prompt,
                role: "user",
                state: "success",
                date: moment().format("HH:mm A"),
            })
        );

        const agentMessageId = _.uniqueId();
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: agentMessageId,
                role: "assistant",
                date: moment().format("HH:mm A"),
                state: "loading",
            })
        );

        try {
            const result = await chatAction();

            if (result.ChatId) {
                dispatch(chatAiAgentActions.chatIdSet(result.ChatId));
            }
            setValue("prompt", "");
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    content: JSON.stringify(result.Response, null, 2),
                    state: "success",
                    usage: result.Usage,
                })
            );
            dispatch(chatAiAgentActions.getHistoryDocuments({ databaseName, agentName }));
        } catch {
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    state: "error",
                })
            );
        }
    });

    const startChatAction = (): Promise<ChatResult> => {
        return aiAgentService.startAiAgent(databaseName, agentName, {
            Parameters: Object.fromEntries(formValues.parameters.map((x) => [x.name, x.value])),
            Prompt: formValues.prompt,
        });
    };

    const resumeChatAction = (): Promise<ChatResult> => {
        return aiAgentService.resumeAiAgent(databaseName, agentName, chatId, {
            ToolResponse: null, // TODO
            UserPrompt: formValues.prompt,
        });
    };

    const handleSend = async () => {
        return tryHandleSubmit(async () => {
            if (!chatId) {
                return asyncChat.execute(startChatAction);
            } else {
                return asyncChat.execute(resumeChatAction);
            }
        });
    };

    // Get data on load
    useEffect(() => {
        dispatch(chatAiAgentActions.getHistoryDocuments({ databaseName, agentName }));
        dispatch(chatAiAgentActions.getConfig({ databaseName, agentName })).then((action: TODO) => {
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
        dispatch(chatAiAgentActions.chatIdSet(null));
    };

    if (!agentName) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
    }

    return (
        <div className="content-padding h-100 vstack">
            <div className="hstack justify-content-between align-items-start">
                <AboutViewHeading title={agentName} icon="ai-agents" marginBottom={3} className="text-truncate" />
                <ChatAiAgentInfoHub />
            </div>
            <div className="hstack gap-2 mb-2">
                <a className="btn btn-secondary rounded-pill" href={appUrl.forAiAgents(databaseName)}>
                    <Icon icon="cancel" /> Cancel
                </a>
                <Button variant="primary" className="rounded-pill" onClick={handleAddChat}>
                    <Icon icon="plus" /> Add chat
                </Button>
            </div>

            <div className="flex-grow-1 hstack">
                <div style={{ width: "250px" }} className="p-3 border border-secondary panel-bg-2 h-100 rounded-2">
                    <h5 className="text-muted">Chat history</h5>
                    {historyDocuments.status === "success" && (
                        <div className="vstack gap-2">
                            {historyDocuments.data.map((doc) => (
                                <div
                                    key={doc["@metadata"]["@id"]}
                                    onClick={() =>
                                        dispatch(
                                            chatAiAgentActions.historyChatSelected({ docId: doc["@metadata"]["@id"] })
                                        )
                                    }
                                    className="hover-filter cursor-pointer text-truncate"
                                >
                                    {doc.Messages?.find((x: { role: string }) => x.role === "user")?.content}
                                </div>
                            ))}
                        </div>
                    )}
                </div>
                <form className="flex-grow-1 vstack" onSubmit={handleSubmit(handleSend)}>
                    <div ref={messagesPanelRef} className="flex-grow-1 overflow-auto ps-2">
                        {messages.length === 0 ? (
                            <div className="p-5">
                                <AiAgentParametersField
                                    control={control}
                                    name="parameters"
                                    value={formValues.parameters}
                                />
                            </div>
                        ) : (
                            <AiAgentMessages messages={messages} />
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
        </div>
    );
}
