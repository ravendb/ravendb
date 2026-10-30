import { useEffect, useRef } from "react";

// When active becomes true, scrolls the active item to the middle of its scroll container, so a
// reopened dropdown shows the current selection without hunting. Attach listRef to the scroll
// container and activeRef to the currently-selected item.
export function useScrollActiveIntoView<TList extends HTMLElement, TActive extends HTMLElement>(active: boolean) {
    const listRef = useRef<TList>(null);
    const activeRef = useRef<TActive>(null);

    useEffect(() => {
        if (!active) {
            return;
        }
        const list = listRef.current;
        const item = activeRef.current;
        if (list && item) {
            list.scrollTop = item.offsetTop - list.clientHeight / 2 + item.clientHeight / 2;
        }
    }, [active]);

    return { listRef, activeRef };
}
