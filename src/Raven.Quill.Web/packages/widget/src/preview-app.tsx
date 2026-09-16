import { useEffect, useState } from "react";
import { ChatWidget } from "@/chat-widget";
import { announceReady, readPreviewTheme, type PreviewView } from "@/preview-channel";
import { PREVIEW_TRANSCRIPT } from "@/preview-transcript";
import { useCustomCss } from "@/use-custom-css";
import { useDocumentFontSize } from "@/use-document-font-size";
import { useResolvedAppearance } from "@/use-resolved-appearance";
import { DEFAULT_THEME, resolveFontSizeRem, type ResolvedAppearance, type WidgetTheme } from "@/widget-theme";

/** Preview mode is same-origin in the dashboard today, but the handshake validates its peer anyway so the
 *  future loader script inherits a channel that is already correct cross-origin. */
export function PreviewApp() {
    const [theme, setTheme] = useState<WidgetTheme>(DEFAULT_THEME);
    const [appearanceOverride, setAppearanceOverride] = useState<ResolvedAppearance | null>(null);
    const [view, setView] = useState<PreviewView>("Conversation");
    const systemAppearance = useResolvedAppearance(theme.appearance);

    useCustomCss(theme.customCss);
    useDocumentFontSize(resolveFontSizeRem(theme));

    useEffect(() => {
        const expectedOrigin = window.location.origin;

        const onMessage = (event: MessageEvent) => {
            const payload = readPreviewTheme(event, expectedOrigin);
            if (payload === null) return;
            setTheme(payload.theme);
            setAppearanceOverride(payload.appearanceOverride);
            setView(payload.view);
        };

        window.addEventListener("message", onMessage);
        announceReady(expectedOrigin);
        return () => window.removeEventListener("message", onMessage);
    }, []);

    return (
        // Keyed by view because `useChat` seeds its transcript once; remounting swaps welcome <-> conversation.
        <ChatWidget
            key={view}
            theme={theme}
            appearance={appearanceOverride ?? systemAppearance}
            history={view === "Welcome" ? [] : PREVIEW_TRANSCRIPT}
            chatUrl={null}
            timeLabel="Today"
        />
    );
}
