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
