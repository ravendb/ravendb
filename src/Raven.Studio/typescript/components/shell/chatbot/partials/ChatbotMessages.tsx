import { Icon } from "components/common/Icon";
import { ChatbotMessage } from "../store/chatbotSlice";
import { LazyLoad } from "components/common/LazyLoad";
import moment from "moment";
import assertUnreachable from "components/utils/assertUnreachable";

interface ChatbotMessagesProps {
    messages: ChatbotMessage[];
}

export default function ChatbotMessages({ messages }: ChatbotMessagesProps) {
    return (
        <div className="flex-grow-1 overflow-y-auto vstack gap-2">
            {messages.map((message) => (
                <div key={message.id} className="px-2">
                    <AiAgentMessage message={message} />
                </div>
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    message: ChatbotMessage;
}

function AiAgentMessage({ message }: AiAgentMessageProps) {
    switch (message.role) {
        case "user":
            return <UserMessage message={message} />;
        case "assistant":
            return <AgentMessage message={message} />;
        default:
            return assertUnreachable(message.role);
    }
}

interface UserMessageProps {
    message: ChatbotMessage;
}

function UserMessage({ message }: UserMessageProps) {
    return (
        <div className="hstack justify-content-end">
            <div
                className="text-emphasis text-end bg-faded-primary p-2 border-radius-xs border border-primary"
                style={{ maxWidth: "75%" }}
            >
                <div className="overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </div>
            </div>
        </div>
    );
}

interface AgentMessageProps {
    message: ChatbotMessage;
}

function AgentMessage({ message }: AgentMessageProps) {
    if (message.state === "loading") {
        return (
            <LazyLoad active>
                <div style={{ height: "100px", width: "100%" }}>Loading...</div>
            </LazyLoad>
        );
    }

    const formattedThinkingTime = message.thinkingTimeInMs
        ? `${moment.duration(message.thinkingTimeInMs).asSeconds().toFixed(2)}s`
        : null;

    return (
        <div>
            {formattedThinkingTime && (
                <div className="text-muted">
                    Though for {formattedThinkingTime}
                    <Icon icon="chevron-right" margin="ms-1" size="sm" />
                </div>
            )}
            <div className="mt-1" style={{ whiteSpace: "pre-wrap" }}>
                {message.content}
            </div>
        </div>
    );
}
