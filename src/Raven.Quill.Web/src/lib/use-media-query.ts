import { useSyncExternalStore } from "react";

function getMediaQueryList(query: string) {
    return window.matchMedia(query);
}

export function useMediaQuery(query: string) {
    return useSyncExternalStore(
        (onStoreChange) => {
            const mediaQueryList = getMediaQueryList(query);
            mediaQueryList.addEventListener("change", onStoreChange);

            return () => {
                mediaQueryList.removeEventListener("change", onStoreChange);
            };
        },
        () => getMediaQueryList(query).matches,
        () => false,
    );
}

/** Below the `lg` breakpoint the shell has no room to dock both the sidebar and a side panel. */
export const COMPACT_LAYOUT_MEDIA_QUERY = "(max-width: 63.999rem)";
