import { useLayoutEffect, useRef, useState, type ReactNode } from "react";
import { ArrowDownIcon } from "@/components/icons";

/** Browsers report fractional scroll offsets, and a streaming turn grows the feed between frames, so
 *  "at the bottom" needs slack rather than an exact comparison. */
const AT_BOTTOM_SLACK_PX = 32;

function prefersReducedMotion(): boolean {
    return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
}

type MessageFeedProps = {
    /** Changes whenever the transcript grows or a streaming turn appends text. */
    scrollSignal: unknown;
    children: ReactNode;
};

export function MessageFeed({ scrollSignal, children }: MessageFeedProps) {
    const scrollRef = useRef<HTMLDivElement>(null);
    const [isAtBottom, setIsAtBottom] = useState(true);

    // Following new content is only correct while the reader is already at the bottom - yanking them back
    // mid-scroll is the single most common thing chat widgets get wrong.
    useLayoutEffect(() => {
        const element = scrollRef.current;
        if (element === null || isAtBottom === false) return;
        element.scrollTop = element.scrollHeight;
    }, [scrollSignal, isAtBottom]);

    const scrollToLatest = () => {
        const element = scrollRef.current;
        if (element === null) return;
        element.scrollTo({
            top: element.scrollHeight,
            behavior: prefersReducedMotion() ? "auto" : "smooth",
        });
    };

    return (
        <div className="relative min-h-0 flex-1">
            <div
                ref={scrollRef}
                role="log"
                aria-live="polite"
                aria-label="Conversation"
                onScroll={(event) => {
                    const { scrollTop, scrollHeight, clientHeight } = event.currentTarget;
                    setIsAtBottom(scrollHeight - scrollTop - clientHeight <= AT_BOTTOM_SLACK_PX);
                }}
                className="h-full overflow-y-auto px-[var(--rq-pad-x)] py-[var(--rq-pad-y)]"
            >
                {children}
            </div>

            {isAtBottom === false && (
                <button
                    type="button"
                    onClick={scrollToLatest}
                    className="rounded-rq-pill border-rq-border bg-rq-surface hover:border-rq-accent focus-visible:ring-rq-accent absolute bottom-3 left-1/2 inline-flex -translate-x-1/2 items-center gap-1.5 border px-3 py-1.5 text-xs font-medium shadow-sm transition-colors focus-visible:ring-2 focus-visible:outline-none"
                >
                    <ArrowDownIcon className="size-3.5" />
                    Show latest
                </button>
            )}
        </div>
    );
}
