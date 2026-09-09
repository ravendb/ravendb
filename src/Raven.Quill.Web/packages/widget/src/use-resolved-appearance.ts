import { useSyncExternalStore } from "react";
import type { ResolvedAppearance, WidgetAppearance } from "@/widget-theme";

const DARK_QUERY = "(prefers-color-scheme: dark)";

function subscribe(onStoreChange: () => void): () => void {
    const query = window.matchMedia(DARK_QUERY);
    query.addEventListener("change", onStoreChange);
    return () => query.removeEventListener("change", onStoreChange);
}

function getSnapshot(): boolean {
    return window.matchMedia(DARK_QUERY).matches;
}

/** `System` follows the host visitor's OS preference, which can change while the widget is open. */
export function useResolvedAppearance(appearance: WidgetAppearance): ResolvedAppearance {
    const prefersDark = useSyncExternalStore(subscribe, getSnapshot);
    if (appearance === "Light" || appearance === "Dark") return appearance;
    return prefersDark ? "Dark" : "Light";
}
