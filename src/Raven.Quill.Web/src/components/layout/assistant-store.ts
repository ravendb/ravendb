import { create } from "zustand";
import { COMPACT_LAYOUT_MEDIA_QUERY, useMediaQuery } from "@/lib/use-media-query";

const ASSISTANT_OPEN_STORAGE_KEY = "assistant-open";
const ASSISTANT_PINNED_STORAGE_KEY = "assistant-pinned";
const ASSISTANT_WIDTH_STORAGE_KEY = "assistant-width";
const ASSISTANT_HEIGHT_STORAGE_KEY = "assistant-height";

/** Labels the assistant landmark in the app shell with the panel's own heading. */
export const ASSISTANT_PANEL_TITLE_ID = "assistant-panel-title";

export const ASSISTANT_MIN_WIDTH_PX = 320;
export const ASSISTANT_MAX_WIDTH_PX = 640;
const ASSISTANT_DEFAULT_WIDTH_PX = 384;

export const ASSISTANT_MIN_HEIGHT_PX = 320;
export const ASSISTANT_MAX_HEIGHT_PX = 960;
const ASSISTANT_DEFAULT_HEIGHT_PX = 704;

function clampSize(sizePx: number, minPx: number, maxPx: number) {
    return Math.min(maxPx, Math.max(minPx, Math.round(sizePx)));
}

function readStoredSize(storageKey: string, defaultPx: number, minPx: number, maxPx: number) {
    const storedPx = Number(localStorage.getItem(storageKey));
    return Number.isFinite(storedPx) && storedPx > 0 ? clampSize(storedPx, minPx, maxPx) : defaultPx;
}

type AssistantState = {
    isOpen: boolean;
    isPinned: boolean;
    /** True while a resize handle is being dragged, so the shell can suspend its column transition. */
    isResizing: boolean;
    /** Bumped on every open so the composer can pull focus without stealing it on page load. */
    openCount: number;
    widthPx: number;
    /** Panel height in the unpinned (floating) mode; when pinned the grid row sizes it. */
    heightPx: number;
    setOpen: (isOpen: boolean) => void;
    setPinned: (isPinned: boolean) => void;
    setResizing: (isResizing: boolean) => void;
    setWidth: (widthPx: number) => void;
    setHeight: (heightPx: number) => void;
};

export const useAssistantStore = create<AssistantState>((set) => ({
    isOpen: localStorage.getItem(ASSISTANT_OPEN_STORAGE_KEY) === "true",
    isPinned: localStorage.getItem(ASSISTANT_PINNED_STORAGE_KEY) !== "false",
    isResizing: false,
    openCount: 0,
    widthPx: readStoredSize(
        ASSISTANT_WIDTH_STORAGE_KEY,
        ASSISTANT_DEFAULT_WIDTH_PX,
        ASSISTANT_MIN_WIDTH_PX,
        ASSISTANT_MAX_WIDTH_PX,
    ),
    heightPx: readStoredSize(
        ASSISTANT_HEIGHT_STORAGE_KEY,
        ASSISTANT_DEFAULT_HEIGHT_PX,
        ASSISTANT_MIN_HEIGHT_PX,
        ASSISTANT_MAX_HEIGHT_PX,
    ),
    setOpen: (isOpen) =>
        set((state) => {
            localStorage.setItem(ASSISTANT_OPEN_STORAGE_KEY, String(isOpen));
            return { isOpen, openCount: isOpen ? state.openCount + 1 : state.openCount };
        }),
    setPinned: (isPinned) => {
        localStorage.setItem(ASSISTANT_PINNED_STORAGE_KEY, String(isPinned));
        set({ isPinned });
    },
    setResizing: (isResizing) => set({ isResizing }),
    setWidth: (widthPx) => {
        const clampedPx = clampSize(widthPx, ASSISTANT_MIN_WIDTH_PX, ASSISTANT_MAX_WIDTH_PX);
        localStorage.setItem(ASSISTANT_WIDTH_STORAGE_KEY, String(clampedPx));
        set({ widthPx: clampedPx });
    },
    setHeight: (heightPx) => {
        const clampedPx = clampSize(heightPx, ASSISTANT_MIN_HEIGHT_PX, ASSISTANT_MAX_HEIGHT_PX);
        localStorage.setItem(ASSISTANT_HEIGHT_STORAGE_KEY, String(clampedPx));
        set({ heightPx: clampedPx });
    },
}));

// A docked panel would leave the page with no room on narrow viewports, so below the shell's
// breakpoint the panel always floats and the pin preference is kept but not applied.
export function useAssistantPinning() {
    const isPinned = useAssistantStore((state) => state.isPinned);
    const canPin = !useMediaQuery(COMPACT_LAYOUT_MEDIA_QUERY);

    return { canPin, isPinned: canPin && isPinned };
}
