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
const ASSISTANT_DEFAULT_HEIGHT_PX = 704;
/** Mirrors the `calc(100svh - 4rem)` cap the floating panel renders itself with in the app shell. */
const ASSISTANT_VIEWPORT_MARGIN_PX = 64;

export function assistantMaxHeightPx() {
    return Math.max(ASSISTANT_MIN_HEIGHT_PX, window.innerHeight - ASSISTANT_VIEWPORT_MARGIN_PX);
}

export function clampAssistantSize(sizePx: number, minPx: number, maxPx: number) {
    return Math.min(maxPx, Math.max(minPx, Math.round(sizePx)));
}

function persistSize(storageKey: string, sizePx: number) {
    localStorage.setItem(storageKey, String(sizePx));
}

function readStoredSize(storageKey: string, defaultPx: number, minPx: number, maxPx = Number.POSITIVE_INFINITY) {
    const storedPx = Number(localStorage.getItem(storageKey));
    return Number.isFinite(storedPx) && storedPx > 0 ? clampAssistantSize(storedPx, minPx, maxPx) : defaultPx;
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

export const useAssistantStore = create<AssistantState>((set, get) => ({
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
    heightPx: readStoredSize(ASSISTANT_HEIGHT_STORAGE_KEY, ASSISTANT_DEFAULT_HEIGHT_PX, ASSISTANT_MIN_HEIGHT_PX),
    setOpen: (isOpen) =>
        set((state) => {
            localStorage.setItem(ASSISTANT_OPEN_STORAGE_KEY, String(isOpen));
            return { isOpen, openCount: isOpen ? state.openCount + 1 : state.openCount };
        }),
    setPinned: (isPinned) => {
        localStorage.setItem(ASSISTANT_PINNED_STORAGE_KEY, String(isPinned));
        set({ isPinned });
    },
    setResizing: (isResizing) => {
        set({ isResizing });
        if (isResizing) {
            return;
        }

        const { widthPx, heightPx } = get();
        persistSize(ASSISTANT_WIDTH_STORAGE_KEY, widthPx);
        persistSize(ASSISTANT_HEIGHT_STORAGE_KEY, heightPx);
    },
    setWidth: (widthPx) => {
        const clampedPx = clampAssistantSize(widthPx, ASSISTANT_MIN_WIDTH_PX, ASSISTANT_MAX_WIDTH_PX);
        set({ widthPx: clampedPx });
        if (!get().isResizing) {
            persistSize(ASSISTANT_WIDTH_STORAGE_KEY, clampedPx);
        }
    },
    setHeight: (heightPx) => {
        const clampedPx = clampAssistantSize(heightPx, ASSISTANT_MIN_HEIGHT_PX, assistantMaxHeightPx());
        set({ heightPx: clampedPx });
        if (!get().isResizing) {
            persistSize(ASSISTANT_HEIGHT_STORAGE_KEY, clampedPx);
        }
    },
}));

// A docked panel would leave the page with no room on narrow viewports, so below the shell's
// breakpoint the panel always floats and the pin preference is kept but not applied.
export function useAssistantPinning() {
    const isPinned = useAssistantStore((state) => state.isPinned);
    const canPin = !useMediaQuery(COMPACT_LAYOUT_MEDIA_QUERY);

    return { canPin, isPinned: canPin && isPinned };
}
