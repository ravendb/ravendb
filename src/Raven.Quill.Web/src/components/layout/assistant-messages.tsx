import { useEffect, useRef } from "react";
import { ExternalLink, Sparkles } from "lucide-react";
import { Streamdown } from "streamdown";
import { useAssistantChatStore, type AssistantMessage } from "@/components/layout/assistant-chat-store";

// How far from the bottom still counts as "following along", so a streaming answer keeps scrolling.
const STICK_TO_BOTTOM_THRESHOLD_PX = 48;

function AssistantMessageItem({ message }: { message: AssistantMessage }) {
    if (message.role === "user") {
        return (
            <div className="ml-auto max-w-[85%] rounded-lg bg-primary px-3 py-2 text-sm whitespace-pre-wrap text-primary-foreground">
                {message.text}
            </div>
        );
    }

    if (message.role === "error") {
        return (
            <div className="max-w-[85%] rounded-lg border border-destructive/40 bg-destructive/10 px-3 py-2 text-sm text-destructive">
                {message.text}
            </div>
        );
    }

    return (
        <div className="rounded-lg py-2 text-sm">
            {message.text === "" ? (
                <span className="animate-pulse text-muted-foreground">Thinking…</span>
            ) : (
                <Streamdown>{message.text}</Streamdown>
            )}
            <AssistantRelevantLinks links={message.relevantLinks} />
        </div>
    );
}

function AssistantRelevantLinks({ links }: { links: AssistantMessage["relevantLinks"] }) {
    // The links come straight from the AI service, so a blank url is possible and unlinkable.
    const linkedSources = links?.filter((link) => link.Url?.trim());

    if (!linkedSources || linkedSources.length === 0) {
        return null;
    }

    return (
        <div className="mt-2 border-t pt-2">
            <p className="text-xs font-medium text-muted-foreground">Related documentation</p>
            <ul className="mt-1 flex flex-col gap-1">
                {linkedSources.map((link) => (
                    <li key={link.Url}>
                        <a
                            href={link.Url}
                            target="_blank"
                            rel="noreferrer"
                            className="flex items-start gap-1 text-xs text-primary hover:underline"
                        >
                            <ExternalLink className="mt-0.5 size-3 shrink-0" aria-hidden="true" />
                            {link.Title || link.Url}
                        </a>
                    </li>
                ))}
            </ul>
        </div>
    );
}

export function AssistantMessages() {
    const messages = useAssistantChatStore((state) => state.messages);
    const scrollRef = useRef<HTMLDivElement>(null);
    // Content growing does not fire a scroll event, so the operator's last scroll position is what
    // decides whether to follow the stream: scrolling up to re-read must not snap back down.
    const isFollowingStreamRef = useRef(true);

    // DOM side effect with no event to hang it on: keep the newest message in view as it arrives
    // and while it streams in. Scrolling this container directly rather than calling
    // scrollIntoView keeps the app shell — itself a scroll container — from scrolling along.
    useEffect(() => {
        const scrollArea = scrollRef.current;
        if (scrollArea && isFollowingStreamRef.current) {
            scrollArea.scrollTop = scrollArea.scrollHeight;
        }
    }, [messages]);

    return (
        <div
            ref={scrollRef}
            className="flex-1 overflow-y-auto p-3"
            onScroll={(event) => {
                const { scrollTop, scrollHeight, clientHeight } = event.currentTarget;
                isFollowingStreamRef.current = scrollHeight - scrollTop - clientHeight <= STICK_TO_BOTTOM_THRESHOLD_PX;
            }}
        >
            {messages.length === 0 ? (
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
                    {messages.map((message) => (
                        <AssistantMessageItem key={message.id} message={message} />
                    ))}
                </div>
            )}
        </div>
    );
}
