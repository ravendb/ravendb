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
import ChatbotAskAiMessageEndpoints from "./askAi/ChatbotAskAiMessageEndpoints";
import ChatbotAskAiMessageRelevantLinks from "./askAi/ChatbotAskAiMessageRelevantLinks";
import ChatbotAskAiMessageFollowUpQuestions from "./askAi/ChatbotAskAiMessageFollowUpQuestions";
import { TextShimmer } from "components/common/TextShimmer";
import { Icon } from "components/common/Icon";
import copyToClipboard from "common/copyToClipboard";
import "./ChatbotMessages.scss";
import classNames from "classnames";

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
        <div ref={messagesRef} className="chatbot-body flex-grow-1 overflow-y-auto vstack gap-2 px-2">
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
        <div className="text-break" style={{ minHeight: isLastMessage ? "-webkit-fill-available" : "unset" }}>
            <AgentMessageBody message={message} />
        </div>
    );
}

function AgentMessageBody({ message }: AgentMessageProps) {
    const dispatch = useAppDispatch();

    const contentTypewriter = useTypewriter({
        text: message.content,
        isDone: message.thinkingTimeInMs != null,
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
        return (
            <AiAssistantConsentStatusChecker
                onConsentGiven={() => dispatch(chatbotActions.retryRunChat())}
                hasAsciiIcon
            />
        );
    }

    if (message.state === "RequestTooLarge" || message.state === "Aborted" || message.state === "InternalError") {
        const errorMessageId = message.errorMessage?.match(/id: '([^']+)'/)?.[1];

        return (
            <RichAlert variant="danger">
                {message.errorMessage}
                {errorMessageId && (
                    <Button
                        variant="link"
                        onClick={() => copyToClipboard.copy(errorMessageId, `Copied error ID to clipboard`)}
                        title="Copy error ID"
                    >
                        <Icon icon="copy" />
                    </Button>
                )}
            </RichAlert>
        );
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
            <div className="hstack text-muted gap-1 pb-1">
                <svg
                    width="12"
                    height="12"
                    viewBox="0 0 1024 1024"
                    fill="none"
                    xmlns="http://www.w3.org/2000/svg"
                    className={classNames(message.thinkingTimeInMs === null ? "thinking-icon" : "")}
                >
                    <path
                        d="M463.439 145.43C473.744 118.58 503.859 105.167 530.722 115.472C544.507 120.764 555.384 131.658 560.677 145.43L643.666 361.237C647.066 370.036 654.011 376.981 662.809 380.381L877.366 462.929C898.335 470.701 912.184 490.777 911.998 513.133C911.678 534.303 898.469 553.144 878.672 560.676L662.861 643.662C654.063 647.062 647.117 654.008 643.718 662.806L560.663 878.668C558.023 885.597 554.004 891.714 548.986 896.754C539.408 906.376 526.198 912.071 512.041 912.001C490.471 912.107 471.101 898.828 463.422 878.672L380.432 662.864C377.033 654.066 370.087 647.12 361.289 643.721L145.426 560.666C118.578 550.36 105.167 520.242 115.472 493.38C119.157 483.782 125.56 475.596 133.757 469.72C134.004 469.543 134.251 469.366 134.501 469.193C134.891 468.924 135.283 468.658 135.68 468.4C138.722 466.419 141.986 464.745 145.43 463.422L361.237 380.436C363.46 379.577 365.56 378.486 367.518 377.202H367.525C373.32 373.403 377.844 367.864 380.384 361.289L463.439 145.43Z"
                        fill="currentColor"
                    />
                </svg>
                {message.thinkingTimeInMs != null ? (
                    <span>Thought for {moment.duration(message.thinkingTimeInMs).asSeconds().toFixed(2)}s</span>
                ) : (
                    <TextShimmer>Thinking</TextShimmer>
                )}
            </div>
            <div className="pb-1">
                <ChatbotAskAiMarkdown content={contentTypewriter} />
            </div>
            <ChatbotAskAiMessageRelevantLinks links={message.relevantLinks} />
            <ChatbotAskAiMessageFollowUpQuestions questions={message.followUpQuestions} />
        </div>
    );
}
