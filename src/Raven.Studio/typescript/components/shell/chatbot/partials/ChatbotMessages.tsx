import { Icon } from "components/common/Icon";
import { ChatbotMessage } from "../store/chatbotSlice";
import { LazyLoad } from "components/common/LazyLoad";
import moment from "moment";

interface ChatbotMessagesProps {
    messages: ChatbotMessage[];
}

export default function ChatbotMessages({ messages }: ChatbotMessagesProps) {
    return (
        <div className="w-100 vstack gap-2 ai-agent-messages pb-4">
            {messages.map((message) => (
                <AiAgentMessage key={message.id} message={message} />
            ))}
        </div>
    );
}

interface AiAgentMessageProps {
    message: ChatbotMessage;
}

function AiAgentMessage({ message }: AiAgentMessageProps) {
    return (
        <div>
            {message.role === "system" && <SystemMessage message={message} />}
            {message.role === "user" && <UserMessage message={message} />}
            {message.role === "assistant" && <AgentMessage message={message} />}
        </div>
    );
}

interface SystemMessageProps {
    message: ChatbotMessage;
}

function SystemMessage({ message }: SystemMessageProps) {
    // TODO show time?

    return (
        <div className="text-muted">
            {/* <div className="text-center md-label">{message.date}</div> */}
            <div className="p-2 border-start border-secondary d-flex vstack">
                <small>
                    <Icon icon="system" size="xs" />
                    System prompt
                </small>
                <small className="mt-2 overflow-auto" style={{ maxHeight: "200px", whiteSpace: "pre-wrap" }}>
                    {message.content}
                </small>
            </div>
        </div>
    );
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
