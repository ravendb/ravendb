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

export type PreviewDevice = "desktop" | "mobile";

type WebWidgetThemePreviewProps = {
    theme: WidgetTheme;
    appearance: PreviewAppearance;
    device: PreviewDevice;
    className?: string;
};

export function WebWidgetThemePreview({ theme, appearance, device, className }: WebWidgetThemePreviewProps) {
    const iframeRef = useRef<HTMLIFrameElement>(null);
    const [isReady, setIsReady] = useState(false);

    // The widget posts `ready` once it has mounted its message listener; pushing before that is dropped.
    useEffect(() => {
        const onMessage = (event: MessageEvent) => {
            if (event.origin !== window.location.origin) return;
            const data = event.data as { source?: string; version?: number; type?: string } | null;
            if (data?.source !== ENVELOPE_SOURCE || data.version !== ENVELOPE_VERSION || data.type !== "ready") return;
            setIsReady(true);
        };

        window.addEventListener("message", onMessage);
        return () => window.removeEventListener("message", onMessage);
    }, []);

    useEffect(() => {
        if (!isReady) return;

        const timer = setTimeout(() => {
            iframeRef.current?.contentWindow?.postMessage(
                {
                    source: ENVELOPE_SOURCE,
                    version: ENVELOPE_VERSION,
                    type: "theme",
                    payload: { theme, appearanceOverride: appearance },
                },
                window.location.origin,
            );
        }, PUSH_DEBOUNCE_MS);

        return () => clearTimeout(timer);
    }, [theme, appearance, isReady]);

    return (
        <div
            className={cn(
                "mx-auto overflow-hidden rounded-xl border bg-background shadow-sm transition-[max-width]",
                device === "mobile" ? "max-w-[380px]" : "max-w-[620px]",
                className,
            )}
        >
            <iframe
                ref={iframeRef}
                title="Web widget preview"
                src={PREVIEW_SRC}
                onLoad={() => setIsReady(false)}
                className="h-[600px] w-full border-0"
            />
        </div>
    );
}
