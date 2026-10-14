import { useEffect, useRef, type UIEvent } from "react";

/** How far from the bottom still counts as following along, so a streaming answer keeps scrolling. */
const STICK_TO_BOTTOM_THRESHOLD_PX = 48;

/**
 * Keeps a scroll area at its bottom while its content grows - a reply streaming in, its markdown
 * rendering - unless the operator scrolled up to read. Growing content fires no scroll event, so the
 * content's size is what moves the view, and the operator's last scroll decides whether it may.
 */
export function useStickToBottom<ScrollArea extends HTMLElement, Content extends HTMLElement>() {
    const scrollRef = useRef<ScrollArea>(null);
    const contentRef = useRef<Content>(null);
    const isFollowingRef = useRef(true);

    useEffect(() => {
        const scrollArea = scrollRef.current;
        const content = contentRef.current;

        if (!scrollArea || !content) {
            return;
        }

        const observer = new ResizeObserver(() => {
            if (isFollowingRef.current) {
                scrollArea.scrollTop = scrollArea.scrollHeight;
            }
        });

        observer.observe(scrollArea);
        observer.observe(content);

        return () => observer.disconnect();
    }, []);

    const onScroll = (event: UIEvent<ScrollArea>) => {
        const { scrollTop, scrollHeight, clientHeight } = event.currentTarget;
        isFollowingRef.current = scrollHeight - scrollTop - clientHeight <= STICK_TO_BOTTOM_THRESHOLD_PX;
    };

    /** Back to the bottom and following, e.g. for an exchange the operator just started from higher up. */
    const follow = () => {
        const scrollArea = scrollRef.current;
        isFollowingRef.current = true;

        if (scrollArea) {
            scrollArea.scrollTop = scrollArea.scrollHeight;
        }
    };

    return { scrollRef, contentRef, onScroll, follow };
}
