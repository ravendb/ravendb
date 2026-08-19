import { useEffect, useRef } from "react";
import { Pin, PinOff, Sparkles, Trash2, X } from "lucide-react";
import { useAssistantChatStore } from "@/components/layout/assistant-chat-store";
import { AssistantComposer } from "@/components/layout/assistant-composer";
import { AssistantConsentGate } from "@/components/layout/assistant-consent";
import { AssistantMessages } from "@/components/layout/assistant-messages";
import {
    ASSISTANT_MAX_WIDTH_PX,
    ASSISTANT_MIN_HEIGHT_PX,
    ASSISTANT_MIN_WIDTH_PX,
    ASSISTANT_PANEL_TITLE_ID,
    assistantMaxHeightPx,
    clampAssistantSize,
    useAssistantPinning,
    useAssistantStore,
} from "@/components/layout/assistant-store";
import { useAssistantConsent } from "@/components/layout/use-assistant-consent";
import { Button } from "@/components/shadcn/ui/button";
import { cn } from "@/lib/utils";

const RESIZE_KEYBOARD_STEP_PX = 16;

function AssistantResizeHandle({ axis }: { axis: "width" | "height" }) {
    const isWidthAxis = axis === "width";
    const valuePx = useAssistantStore((state) => (isWidthAxis ? state.widthPx : state.heightPx));
    const setSize = useAssistantStore((state) => (isWidthAxis ? state.setWidth : state.setHeight));
    const setResizing = useAssistantStore((state) => state.setResizing);
    const minPx = isWidthAxis ? ASSISTANT_MIN_WIDTH_PX : ASSISTANT_MIN_HEIGHT_PX;
    const maxPx = isWidthAxis ? ASSISTANT_MAX_WIDTH_PX : assistantMaxHeightPx();
    const effectiveValuePx = clampAssistantSize(valuePx, minPx, maxPx);
    // The panel is anchored to the viewport's right/bottom edge, so dragging the handle
    // left/up (toward smaller client coordinates) grows the panel.
    const growKey = isWidthAxis ? "ArrowLeft" : "ArrowUp";
    const shrinkKey = isWidthAxis ? "ArrowRight" : "ArrowDown";

    // Unmounting mid-drag (pinning hides the height handle) would otherwise leave the shell stuck
    // in its resizing state, with the column transition suppressed for good.
    const stopActiveResizeRef = useRef<(() => void) | null>(null);
    useEffect(() => () => stopActiveResizeRef.current?.(), []);

    function startResize(event: React.PointerEvent<HTMLDivElement>) {
        // A second finger landing on the handle would start a rival drag whose listeners
        // outlive it, leaving the panel stuck in the resizing state.
        if (!event.isPrimary) {
            return;
        }

        event.preventDefault();
        const handle = event.currentTarget;
        const startCoordinate = isWidthAxis ? event.clientX : event.clientY;
        const startValuePx = effectiveValuePx;
        handle.setPointerCapture(event.pointerId);
        setResizing(true);

        const handleMove = (moveEvent: PointerEvent) => {
            const coordinate = isWidthAxis ? moveEvent.clientX : moveEvent.clientY;
            setSize(startValuePx + startCoordinate - coordinate);
        };
        const stopResize = () => {
            handle.removeEventListener("pointermove", handleMove);
            handle.removeEventListener("pointerup", stopResize);
            handle.removeEventListener("pointercancel", stopResize);
            stopActiveResizeRef.current = null;
            setResizing(false);
        };
        handle.addEventListener("pointermove", handleMove);
        handle.addEventListener("pointerup", stopResize);
        handle.addEventListener("pointercancel", stopResize);
        stopActiveResizeRef.current = stopResize;
    }

    return (
        <div
            role="separator"
            tabIndex={0}
            aria-orientation={isWidthAxis ? "vertical" : "horizontal"}
            aria-label={isWidthAxis ? "Resize assistant panel width" : "Resize assistant panel height"}
            aria-valuemin={minPx}
            aria-valuemax={maxPx}
            aria-valuenow={effectiveValuePx}
            className={cn(
                // touch-none stops the browser from claiming the drag as a pan, which would
                // cancel the pointer stream mid-resize on touch devices.
                "absolute z-10 touch-none rounded-full transition-colors hover:bg-ring/40 focus-visible:bg-ring/40 focus-visible:outline-hidden",
                isWidthAxis ? "inset-y-0 -left-1 w-2 cursor-ew-resize" : "inset-x-0 -top-1 h-2 cursor-ns-resize",
            )}
            onPointerDown={startResize}
            onKeyDown={(event) => {
                if (event.key === growKey) {
                    event.preventDefault();
                    setSize(effectiveValuePx + RESIZE_KEYBOARD_STEP_PX);
                } else if (event.key === shrinkKey) {
                    event.preventDefault();
                    setSize(effectiveValuePx - RESIZE_KEYBOARD_STEP_PX);
                }
            }}
        />
    );
}

export function AssistantPanel() {
    const { canPin, isPinned } = useAssistantPinning();
    const setOpen = useAssistantStore((state) => state.setOpen);
    const setPinned = useAssistantStore((state) => state.setPinned);
    const hasMessages = useAssistantChatStore((state) => state.messages.length > 0);
    const clearMessages = useAssistantChatStore((state) => state.clearMessages);
    const hasConsent = useAssistantConsent().data?.status === "Success";

    return (
        <div
            className={cn(
                "relative flex min-h-0 flex-1 flex-col rounded-lg border bg-surface1 dark:bg-surface2",
                isPinned ? "me-2 mb-2" : "shadow-xl",
            )}
            onKeyDown={(event) => {
                // Escape dismisses the floating panel only; the docked one is part of the layout.
                if (event.key === "Escape" && !isPinned) {
                    setOpen(false);
                }
            }}
        >
            <AssistantResizeHandle axis="width" />
            {!isPinned && <AssistantResizeHandle axis="height" />}
            <header className="flex items-center gap-2 border-b px-3 py-2">
                <Sparkles className="size-4 text-primary" aria-hidden="true" />
                <h2 id={ASSISTANT_PANEL_TITLE_ID} className="text-sm font-semibold">
                    AI assistant
                </h2>
                <div className="ml-auto flex items-center gap-1">
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        onClick={clearMessages}
                        disabled={!hasMessages}
                        aria-label="Clear conversation"
                        title="Clear conversation"
                    >
                        <Trash2 aria-hidden="true" />
                    </Button>
                    {canPin && (
                        <Button
                            variant="ghost"
                            size="icon-sm"
                            onClick={() => setPinned(!isPinned)}
                            aria-label={isPinned ? "Unpin AI assistant" : "Pin AI assistant"}
                            title={isPinned ? "Unpin into a floating window" : "Pin into the layout"}
                        >
                            {isPinned ? <PinOff aria-hidden="true" /> : <Pin aria-hidden="true" />}
                        </Button>
                    )}
                    <Button
                        variant="ghost"
                        size="icon-sm"
                        onClick={() => setOpen(false)}
                        aria-label="Close AI assistant"
                        title="Close AI assistant"
                    >
                        <X aria-hidden="true" />
                    </Button>
                </div>
            </header>

            {hasConsent ? (
                <>
                    <AssistantMessages />
                    <AssistantComposer />
                </>
            ) : (
                <AssistantConsentGate />
            )}
        </div>
    );
}
