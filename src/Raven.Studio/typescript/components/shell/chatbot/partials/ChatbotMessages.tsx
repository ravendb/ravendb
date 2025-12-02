import { chatbotActions, ChatbotAssistantMessage, ChatbotUserMessage } from "../store/chatbotSlice";
import { LazyLoad } from "components/common/LazyLoad";
import moment from "moment";
import assertUnreachable from "components/utils/assertUnreachable";
import { useEffect, useRef } from "react";
import Button from "react-bootstrap/Button";
import RichAlert from "components/common/RichAlert";
import { useAppDispatch, useAppSelector } from "components/store";
import { chatbotSelectors } from "../store/chatbotSlice";
import { aiAssistantConstants } from "components/common/aiAssistant/aiAssistantConstants";
import AiAssistantConsentStatusChecker from "components/common/aiAssistant/AiAssistantConsentStatusChecker";
import useTypewriter from "components/hooks/useTypewriter";
import ChatbotAskAiAttachedContext from "./askAi/ChatbotAskAiAttachedContext";
import ChatbotAskAiMarkdown from "./askAi/ChatbotAskAiMarkdown";
import ChatbotAskAiMessageAdditionalContext from "./askAi/ChatbotAskAiMessageAdditionalContext";
import ChatbotAskAiMessageEndpoints from "./askAi/ChatbotAskAiMessageEndpoints";
import ChatbotAskAiMessageRelevantLinks from "./askAi/ChatbotAskAiMessageRelevantLinks";
import ChatbotAskAiMessageFollowUpQuestions from "./askAi/ChatbotAskAiMessageFollowUpQuestions";

export default function ChatbotMessages() {
    const messagesRef = useRef<HTMLDivElement>(null);

    const messageIds = useAppSelector(chatbotSelectors.messageIds);
    const oneBeforeLastMessageRole = useAppSelector(chatbotSelectors.oneBeforeLastMessageRole);

    // Scroll to the bottom when messages are updated
    useEffect(() => {
        const current = messagesRef.current;
        if (!current) {
            return;
        }

        let top = current.scrollHeight - current.clientHeight;

        if (oneBeforeLastMessageRole === "user") {
            top -= 50; // height to see last line of user message
        }

        current.scrollTo({ top, behavior: "smooth" });
    }, [messageIds.length]);

    return (
        <div ref={messagesRef} className="flex-grow-1 overflow-y-auto vstack gap-2 px-2">
            {messageIds.map((id) => (
                <AiAgentMessage key={id} id={id} />
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    id: string;
}

function AiAgentMessage({ id }: AiAgentMessageProps) {
    const message = useAppSelector((state) => chatbotSelectors.messageById(state, id));
    const role = message.role;

    switch (role) {
        case "user":
            return <UserMessage message={message} />;
        case "assistant":
            return <AgentMessage message={message} />;
        default:
            return assertUnreachable(role);
    }
}

interface UserMessageProps {
    message: ChatbotUserMessage;
}

function UserMessage({ message }: UserMessageProps) {
    return (
        <div className="hstack justify-content-end">
            <div
                className="text-emphasis bg-faded-primary p-2 border-radius-xs border border-primary"
                style={{ maxWidth: "75%" }}
            >
                <ChatbotAskAiAttachedContext attachedContexts={message.attachedContexts} isReadOnly className="mb-1" />
                <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </div>
            </div>
        </div>
    );
}

interface AgentMessageProps {
    message: ChatbotAssistantMessage;
}

function AgentMessage({ message }: AgentMessageProps) {
    const isLastMessage = useAppSelector((state) => chatbotSelectors.isLastMessage(state, message.id));

    return (
        <div style={{ minHeight: isLastMessage ? "-webkit-fill-available" : "unset" }}>
            <AgentMessageBody message={message} />
        </div>
    );
}

function AgentMessageBody({ message }: AgentMessageProps) {
    const dispatch = useAppDispatch();

    const contentTypewriter = useTypewriter({
        text: message.content,
    });

    if (message.state === "Loading") {
        return (
            <LazyLoad active>
                <div style={{ height: "100px", width: "100%" }} />
            </LazyLoad>
        );
    }

    if (message.state === "InvalidData") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidData}</RichAlert>;
    }

    if (message.state === "InvalidCredentials") {
        return <RichAlert variant="danger">{aiAssistantConstants.invalidCredentials}</RichAlert>;
    }

    if (message.state === "OutOfTokens") {
        return <RichAlert variant="danger">{aiAssistantConstants.outOfTokens}</RichAlert>;
    }

    if (message.state === "ConsentRequired") {
        return <AiAssistantConsentStatusChecker onConsentGiven={() => dispatch(chatbotActions.retryRunChat())} />;
    }

    if (message.state === "RequestTooLarge") {
        return <RichAlert variant="danger">{message.errorMessage}</RichAlert>;
    }

    if (message.state === "Error") {
        return (
            <RichAlert variant="danger">
                {message.errorMessage ?? "Failed to get response from AI Assistant."}{" "}
                <Button variant="link" className="px-0" onClick={() => dispatch(chatbotActions.retryRunChat())}>
                    Please try again
                </Button>
            </RichAlert>
        );
    }

    if (Object.keys(message.additionalContext).length > 0) {
        return (
            <ChatbotAskAiMessageAdditionalContext
                id={message.id}
                additionalContext={message.additionalContext}
                userActionState={message.userActionState}
            />
        );
    }

    if (message.endpoints.length > 0) {
        return (
            <ChatbotAskAiMessageEndpoints
                id={message.id}
                endpoints={message.endpoints}
                userActionState={message.userActionState}
            />
        );
    }

    return (
        <div>
            {message.thinkingTimeInMs != null ? (
                <div className="text-muted">
                    Thought for {moment.duration(message.thinkingTimeInMs).asSeconds().toFixed(2)}s
                </div>
            ) : (
                <div className="text-muted">Thinking</div>
            )}
            <div className="mt-1">
                <ChatbotAskAiMarkdown content={contentTypewriter} />
            </div>
            <ChatbotAskAiMessageRelevantLinks links={message.relevantLinks} />
            <ChatbotAskAiMessageFollowUpQuestions questions={message.followUpQuestions} />
        </div>
    );
}
