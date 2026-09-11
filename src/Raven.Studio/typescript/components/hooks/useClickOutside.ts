import { RefObject, useEffect } from "react";

// Calls onOutside when a mousedown lands outside ref, while enabled.
export function useClickOutside(ref: RefObject<HTMLElement>, enabled: boolean, onOutside: () => void) {
    useEffect(() => {
        if (!enabled) {
            return;
        }
        const handle = (e: MouseEvent) => {
            if (ref.current && !ref.current.contains(e.target as Node)) {
                onOutside();
            }
        };
        document.addEventListener("mousedown", handle);
        return () => document.removeEventListener("mousedown", handle);
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, [enabled]);
}
