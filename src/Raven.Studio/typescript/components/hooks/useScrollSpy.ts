import { useEffect, useRef, useState } from "react";

interface UseScrollSpyOptions {
    root?: Element | null;
    rootMargin?: string;
}

// Highlights the section currently near the top of the scroll area. `ids` is the ordered list of
// section element ids; the active id is the first id (document order) that is currently intersecting.
//
// Classic scroll-spy caveat: a short last section can never scroll up far enough to cross the
// IntersectionObserver's top threshold, so on its own it would never become active. To handle that
// we also watch the scroll position and force the *last* id active once the scroll area bottoms out.
export function useScrollSpy(ids: string[], options?: UseScrollSpyOptions): string | null {
    const { root = null, rootMargin = "0px 0px -70% 0px" } = options ?? {};
    const idsKey = ids.join("|");
    const [activeId, setActiveId] = useState<string | null>(ids[0] ?? null);
    const visibleRef = useRef<Set<string>>(new Set());

    useEffect(() => {
        setActiveId((current) => (current && ids.includes(current) ? current : (ids[0] ?? null)));

        if (ids.length === 0) {
            return undefined;
        }

        const visible = visibleRef.current;
        visible.clear();

        const scrollElement = (): Element | null => root ?? document.scrollingElement ?? document.documentElement;

        const isAtBottom = (): boolean => {
            const el = scrollElement();
            // within a couple of px of the end (rounding-safe)
            return !!el && el.scrollHeight - el.scrollTop - el.clientHeight <= 2;
        };

        const recompute = () => {
            // bottom-of-scroll wins: guarantees the last (possibly short) section can be selected
            if (isAtBottom()) {
                setActiveId(ids[ids.length - 1]);
                return;
            }
            const next = ids.find((id) => visible.has(id));
            if (next) {
                setActiveId(next);
            }
        };

        let observer: IntersectionObserver | undefined;
        if (typeof IntersectionObserver !== "undefined") {
            observer = new IntersectionObserver(
                (entries) => {
                    for (const entry of entries) {
                        const id = (entry.target as HTMLElement).id;
                        if (entry.isIntersecting) {
                            visible.add(id);
                        } else {
                            visible.delete(id);
                        }
                    }
                    recompute();
                },
                { root, rootMargin, threshold: 0 }
            );

            ids.forEach((id) => {
                const el = document.getElementById(id);
                if (el) {
                    observer.observe(el);
                }
            });
        }

        const scrollTarget: Element | Window = root ?? window;
        scrollTarget.addEventListener("scroll", recompute, { passive: true });
        recompute();

        return () => {
            observer?.disconnect();
            scrollTarget.removeEventListener("scroll", recompute);
        };
        // idsKey captures changes to the id set without re-running on array identity churn.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [idsKey, root, rootMargin]);

    return activeId;
}
