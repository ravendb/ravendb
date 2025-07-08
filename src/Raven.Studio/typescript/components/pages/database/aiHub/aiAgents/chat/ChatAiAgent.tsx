import { databaseSelectors } from "components/common/shell/databaseSliceSelectors";
import { useAppUrls } from "components/hooks/useAppUrls";
import { useAppDispatch, useAppSelector } from "components/store";
import router from "plugins/router";
import { chatAiAgentActions, chatAiAgentSelectors } from "./store/chatAiAgentSlice";
import Form from "react-bootstrap/Form";
import ButtonWithSpinner from "components/common/ButtonWithSpinner";
import { useAsyncCallback } from "react-async-hook";
import { useServices } from "components/hooks/useServices";
import { useEffect, useRef } from "react";
import AiAgentMessages from "../partials/AiAgentMessages";
import { Icon } from "components/common/Icon";
import { AboutViewHeading } from "components/common/AboutView";
import ChatAiAgentInfoHub from "./ChatAiAgentInfoHub";
import Button from "react-bootstrap/Button";

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
    const prompt = useAppSelector(chatAiAgentSelectors.prompt);
    const messages = useAppSelector(chatAiAgentSelectors.messages);
    const chatId = useAppSelector(chatAiAgentSelectors.chatId);
    const historyDocuments = useAppSelector(chatAiAgentSelectors.historyDocuments);

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    const asyncChat = useAsyncCallback(async (chatAction: () => Promise<ChatResult>) => {
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: _.uniqueId(),
                text: prompt,
                author: "user",
                state: "success",
            })
        );

        const agentMessageId = _.uniqueId();
        dispatch(
            chatAiAgentActions.messagesAdd({
                id: agentMessageId,
                author: "agent",
                date: new Date(),
                state: "loading",
            })
        );

        try {
            const result = await chatAction();

            if (result.ChatId) {
                dispatch(chatAiAgentActions.chatIdSet(result.ChatId));
            }

            dispatch(chatAiAgentActions.promptSet(""));
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    text: JSON.stringify(result.Response, null, 2),
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
            Parameters: {}, // TODO
            Prompt: prompt,
        });
    };

    const resumeChatAction = (): Promise<ChatResult> => {
        return aiAgentService.resumeAiAgent(databaseName, agentName, chatId, {
            ToolResponse: null, // TODO
            UserPrompt: prompt,
        });
    };

    const handleSend = () => {
        if (!chatId) {
            asyncChat.execute(startChatAction);
        } else {
            asyncChat.execute(resumeChatAction);
        }
    };

    // Get data on load
    useEffect(() => {
        dispatch(chatAiAgentActions.getConfig({ databaseName, agentName }));
        dispatch(chatAiAgentActions.getHistoryDocuments({ databaseName, agentName }));

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
    }, [messages?.length]);

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

            <div className="flex-grow-1 hstack rounded-2 border border-secondary">
                <div
                    style={{ width: "250px" }}
                    className="p-3 border-end border-secondary panel-bg-2 h-100 rounded-start-2"
                >
                    <h4>
                        <Icon icon="clock" />
                        Chat history
                    </h4>
                    <hr className="my-1" />
                    {historyDocuments.status === "success" && (
                        <div className="vstack gap-1">
                            {historyDocuments.data.map((doc) => (
                                <div
                                    key={doc["@metadata"]["@id"]}
                                    onClick={() =>
                                        dispatch(
                                            chatAiAgentActions.historyChatSelected({ docId: doc["@metadata"]["@id"] })
                                        )
                                    }
                                    className="hover-filter cursor-pointer"
                                >
                                    <div className="text-truncate">
                                        {doc.Messages.find((m: { role: string }) => m.role === "user")?.content}
                                    </div>
                                    <div className="text-muted">TODO date</div>
                                </div>
                            ))}
                        </div>
                    )}
                </div>
                <div className="flex-grow-1 vstack panel-bg-1 rounded-end-2">
                    <div ref={messagesPanelRef} className="flex-grow-1 overflow-auto p-2">
                        {messages?.length > 0 && <AiAgentMessages messages={messages} />}
                    </div>
                    <div className="mt-3 p-2 border-top border-secondary">
                        <div className="position-relative">
                            <Form.Control
                                id="prompt"
                                as="textarea"
                                value={prompt}
                                style={{ resize: "none" }}
                                onChange={(e) => dispatch(chatAiAgentActions.promptSet(e.target.value))}
                                placeholder="Message an agent"
                                rows={3}
                                className="rounded-2"
                            />
                            <ButtonWithSpinner
                                variant="primary"
                                icon="arrow-up"
                                isSpinning={asyncChat.loading}
                                disabled={!prompt}
                                onClick={handleSend}
                                className="position-absolute"
                                style={{ right: "10px", bottom: "10px" }}
                            />
                        </div>
                    </div>
                </div>
            </div>
        </div>
    );
}
