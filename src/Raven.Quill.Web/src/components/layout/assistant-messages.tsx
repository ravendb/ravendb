import { useEffect, useRef } from "react";
import { Sparkles } from "lucide-react";
import { Streamdown } from "streamdown";
import { useAssistantChatStore } from "@/components/layout/assistant-chat-store";

function AssistantMessageItem({ id }: { id: string }) {
    const message = useAssistantChatStore((state) => state.messagesById[id]);

    if (!message) {
        return null;
    }

    if (message.role === "user") {
        return (
            <div className="ml-auto max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground">
                {message.text}
            </div>
        );
    }

    return (
        <div className="max-w-[85%] rounded-lg bg-muted px-3 py-2 text-sm">
            <Streamdown>{message.text}</Streamdown>
        </div>
    );
}

export function AssistantMessages() {
    const messageIds = useAssistantChatStore((state) => state.messageIds);
    const lastMessageText = useAssistantChatStore((state) => {
        const lastMessageId = state.messageIds.at(-1);
        return lastMessageId ? state.messagesById[lastMessageId].text : "";
    });
    const scrollRef = useRef<HTMLDivElement>(null);

    // DOM side effect with no event to hang it on: keep the newest message in view as it arrives
    // and while it streams in. Scrolling this container directly rather than calling
    // scrollIntoView keeps the app shell — itself a scroll container — from scrolling along.
    useEffect(() => {
        const scrollArea = scrollRef.current;
        if (scrollArea) {
            scrollArea.scrollTop = scrollArea.scrollHeight;
        }
    }, [messageIds, lastMessageText]);

    return (
        <div ref={scrollRef} className="flex-1 overflow-y-auto p-3">
            {messageIds.length === 0 ? (
                <div className="flex h-full flex-col items-center justify-center gap-3 px-4 text-center">
                    <div className="flex size-12 items-center justify-center rounded-full bg-muted">
                        <Sparkles className="size-5 text-primary" aria-hidden="true" />
                    </div>
                    <div>
                        <p className="text-sm font-medium">How can I help?</p>
                        <p className="text-sm text-muted-foreground">Ask about your apps, conversations, or setup.</p>
                    </div>
                </div>
            ) : (
                <div className="flex flex-col gap-3">
                    {messageIds.map((id) => (
                        <AssistantMessageItem key={id} id={id} />
                    ))}
                </div>
            )}
        </div>
    );
}
