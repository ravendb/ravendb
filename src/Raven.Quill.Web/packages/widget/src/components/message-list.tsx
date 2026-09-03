import { MarkdownMessage } from "@/components/markdown-message";
import { ThinkingIndicator } from "@/components/thinking-indicator";
import type { ChatMessage } from "@/use-chat";

/** The asymmetry is deliberate: a user turn is a tinted bubble, an assistant turn is full-width plain
 *  markdown. Bubbling assistant output would make tables and code blocks unreadable at widget widths. */
function UserBubble({ content }: { content: string }) {
    return (
        <div className="flex justify-end">
            <div className="rounded-rq bg-rq-user-bubble text-rq-user-bubble-fg max-w-[85%] px-3.5 py-[var(--rq-bubble-pad-y)] text-sm leading-[var(--rq-line-height)] whitespace-pre-wrap">
                {content}
            </div>
        </div>
    );
}

function AssistantMessage({ content, isStreaming }: { content: string; isStreaming: boolean }) {
    if (content.length === 0) return isStreaming ? <ThinkingIndicator /> : null;

    return (
        // min-w-0: a grid item sizes to its content by default, so without this a wide table or code block
        // widens the whole feed instead of scrolling inside its own container.
        <div className="min-w-0 text-sm leading-[var(--rq-line-height)]">
            <MarkdownMessage content={content} isStreaming={isStreaming} />
        </div>
    );
}

function TimeSeparator({ label }: { label: string }) {
    return (
        <div className="flex items-center justify-center py-1">
            <span className="text-rq-muted text-[11px] font-medium">{label}</span>
        </div>
    );
}

type MessageListProps = {
    messages: ChatMessage[];
    streamingId: string | null;
    /** Rendered once above the transcript; null hides the separator entirely. */
    timeLabel: string | null;
    errorMessage: string | null;
};

export function MessageList({ messages, streamingId, timeLabel, errorMessage }: MessageListProps) {
    return (
        <div className="grid gap-[var(--rq-gap)]">
            {timeLabel !== null && <TimeSeparator label={timeLabel} />}

            {messages.map((message) =>
                message.role === "user" ? (
                    <UserBubble key={message.id} content={message.content} />
                ) : (
                    <AssistantMessage
                        key={message.id}
                        content={message.content}
                        isStreaming={message.id === streamingId}
                    />
                ),
            )}

            {errorMessage !== null && (
                <div
                    role="alert"
                    className="rounded-rq border-rq-border bg-rq-surface text-rq-muted border px-3.5 py-2.5 text-sm"
                >
                    {errorMessage}
                </div>
            )}
        </div>
    );
}
