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

    const asyncStartChat = useAsyncCallback(async () => {
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
            const result = await aiAgentService.startAiAgent(databaseName, agentName, {
                Parameters: {}, // TODO
                Prompt: prompt,
            });
            dispatch(chatAiAgentActions.chatIdSet(result.ChatId));
            dispatch(chatAiAgentActions.promptSet(""));
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    text: JSON.stringify(result.Response, null, 2),
                    state: "success",
                    usage: result.Usage,
                })
            );
        } catch {
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    state: "error",
                })
            );
        }
    });

    const asyncResumeChat = useAsyncCallback(async () => {
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
            const result = await aiAgentService.resumeAiAgent(databaseName, agentName, chatId, {
                ToolResponse: null, // TODO ask aviv
                UserPrompt: prompt,
            });
            dispatch(chatAiAgentActions.promptSet(""));
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    text: JSON.stringify(result.Response, null, 2),
                    state: "success",
                    usage: result.Usage,
                })
            );
        } catch {
            dispatch(
                chatAiAgentActions.messagesUpdate({
                    id: agentMessageId,
                    state: "error",
                })
            );
        }
    });

    const handleSend = () => {
        if (!chatId) {
            asyncStartChat.execute();
        } else {
            asyncResumeChat.execute();
        }
    };

    useEffect(() => {
        return () => {
            dispatch(chatAiAgentActions.reset());
        };
    }, []);

    const messagesPanelRef = useRef<HTMLDivElement>(null);

    // Scroll to the bottom of the test panel when new messages are added
    useEffect(() => {
        if (messagesPanelRef.current) {
            messagesPanelRef.current.scrollTo({
                top: messagesPanelRef.current.scrollHeight,
                behavior: "smooth",
            });
        }
    }, [messages.length]);

    if (!agentName) {
        router.navigate(appUrl.forAiAgents(databaseName));
        return null;
    }

    return (
        <div className="content-padding h-100 vstack">
            <h1 className="m-0">{agentName}</h1>
            <div ref={messagesPanelRef} className="flex-grow-1 overflow-auto">
                {messages.length > 0 && <AiAgentMessages messages={messages} />}
            </div>
            <div className="mt-3">
                <Form.Control
                    value={prompt}
                    onChange={(e) => dispatch(chatAiAgentActions.promptSet(e.target.value))}
                    placeholder="Message an agent"
                />
                <div className="hstack justify-content-end mt-2">
                    <ButtonWithSpinner
                        variant="primary"
                        icon="arrow-up"
                        isSpinning={asyncStartChat.loading || asyncResumeChat.loading}
                        disabled={!prompt}
                        onClick={handleSend}
                    />
                </div>
            </div>
        </div>
    );
}
