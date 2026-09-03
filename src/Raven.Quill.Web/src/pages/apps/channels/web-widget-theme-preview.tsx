import { useEffect, useRef, useState } from "react";
import type { WidgetTheme } from "@/api/generated/server-api";
import { cn } from "@/lib/utils";

/** The built widget, served from the backend's wwwroot. Preview mode renders a canned transcript and takes
 *  its theme from postMessage, so the editor previews the real widget rather than a look-alike. */
// index.html is named explicitly rather than relying on directory-index resolution, which differs between
// the backend's static file handler and Storybook's dev server.
const PREVIEW_SRC = "/widget/index.html?mode=preview";

const ENVELOPE_SOURCE = "raven-quill";
const ENVELOPE_VERSION = 1;

/** Enough to coalesce typing into one post without the preview feeling detached from the field. */
const PUSH_DEBOUNCE_MS = 120;

export type PreviewAppearance = "Light" | "Dark";

/** Which screen the widget renders: the welcome (empty) state, where the greeting and suggested prompts
 *  live, or the canned conversation that exercises bubbles, tables and code. */
export type PreviewView = "Welcome" | "Conversation";

type WebWidgetThemePreviewProps = {
    theme: WidgetTheme;
    /** The scheme to preview - the editor passes the one whose colors are being edited. */
    appearance: PreviewAppearance;
    view: PreviewView;
    className?: string;
};

export function WebWidgetThemePreview({ theme, appearance, view, className }: WebWidgetThemePreviewProps) {
    const iframeRef = useRef<HTMLIFrameElement>(null);
    // Counts `ready` envelopes rather than latching a boolean: a reloaded iframe posts a fresh `ready`,
    // and the bump re-runs the push effect so the new document gets the current theme. Pushes to a window
    // that is not listening yet are harmlessly dropped.
    const [readyCount, setReadyCount] = useState(0);

    // The widget posts `ready` once it has mounted its message listener; pushing before that is dropped.
    useEffect(() => {
        const onMessage = (event: MessageEvent) => {
            if (event.origin !== window.location.origin) return;
            if (event.source !== iframeRef.current?.contentWindow) return;
            const data = event.data as { source?: string; version?: number; type?: string } | null;
            if (data?.source !== ENVELOPE_SOURCE || data.version !== ENVELOPE_VERSION || data.type !== "ready") return;
            setReadyCount((count) => count + 1);
        };

        window.addEventListener("message", onMessage);
        return () => window.removeEventListener("message", onMessage);
    }, []);

    useEffect(() => {
        if (readyCount === 0) return;

        const timer = setTimeout(() => {
            iframeRef.current?.contentWindow?.postMessage(
                {
                    source: ENVELOPE_SOURCE,
                    version: ENVELOPE_VERSION,
                    type: "theme",
                    payload: { theme, appearanceOverride: appearance, view },
                },
                window.location.origin,
            );
        }, PUSH_DEBOUNCE_MS);

        return () => clearTimeout(timer);
    }, [theme, appearance, view, readyCount]);

    return (
        <div className={cn("overflow-hidden rounded-xl border bg-background shadow-sm", className)}>
            <iframe
                ref={iframeRef}
                title="Web widget preview"
                src={PREVIEW_SRC}
                className="h-[640px] w-full border-0"
            />
        </div>
    );
}
