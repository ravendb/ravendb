import { useEffect, useState } from "react";
import { ChatWidget } from "@/chat-widget";
import { announceReady, readPreviewTheme } from "@/preview-channel";
import { PREVIEW_TRANSCRIPT } from "@/preview-transcript";
import { useResolvedAppearance } from "@/use-resolved-appearance";
import { DEFAULT_THEME, type ResolvedAppearance, type WidgetTheme } from "@/widget-theme";

/** Preview mode is same-origin in the dashboard today, but the handshake validates its peer anyway so the
 *  future loader script inherits a channel that is already correct cross-origin. */
export function PreviewApp() {
    const [theme, setTheme] = useState<WidgetTheme>(DEFAULT_THEME);
    const [appearanceOverride, setAppearanceOverride] = useState<ResolvedAppearance | null>(null);
    const systemAppearance = useResolvedAppearance(theme.appearance);

    useEffect(() => {
        const expectedOrigin = window.location.origin;

        const onMessage = (event: MessageEvent) => {
            const payload = readPreviewTheme(event, expectedOrigin);
            if (payload === null) return;
            setTheme(payload.theme);
            setAppearanceOverride(payload.appearanceOverride);
        };

        window.addEventListener("message", onMessage);
        announceReady(expectedOrigin);
        return () => window.removeEventListener("message", onMessage);
    }, []);

    return (
        <ChatWidget
            theme={theme}
            appearance={appearanceOverride ?? systemAppearance}
            history={PREVIEW_TRANSCRIPT}
            chatUrl={null}
            timeLabel="Today"
        />
    );
}
