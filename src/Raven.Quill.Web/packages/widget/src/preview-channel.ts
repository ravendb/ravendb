import { envelope, isEnvelope, type Envelope } from "@/envelope";
import { normalizeTheme } from "@/widget-config";
import type { ResolvedAppearance, WidgetTheme } from "@/widget-theme";

/** Which screen the preview renders: the welcome (empty) state or the canned conversation. */
export type PreviewView = "Welcome" | "Conversation";

export type PreviewThemePayload = {
    theme: Partial<WidgetTheme>;
    /** The editor previews the scheme whose colors are being edited, whatever the saved appearance says. */
    appearanceOverride?: ResolvedAppearance | null;
    view?: PreviewView;
};

export type PreviewMessage = Envelope<"theme", PreviewThemePayload>;

/** What the widget acts on: the sender's partial theme merged over the defaults. */
export type ResolvedPreviewTheme = {
    theme: WidgetTheme;
    appearanceOverride: ResolvedAppearance | null;
    view: PreviewView;
};

export type WidgetReadyMessage = Envelope<"ready", Record<string, never>>;

/** Written as if the editor were cross-origin even though it is not today, so the future loader script
 *  inherits a handshake that already validates its peer instead of one that has to be retrofitted. */
export function readPreviewTheme(event: MessageEvent, expectedOrigin: string): ResolvedPreviewTheme | null {
    if (event.origin !== expectedOrigin) return null;
    if (isEnvelope(event.data) === false || event.data.type !== "theme") return null;

    const payload = event.data.payload as Partial<PreviewThemePayload> | undefined;
    if (typeof payload !== "object" || payload === null) return null;

    const appearanceOverride = payload.appearanceOverride;
    return {
        theme: normalizeTheme(payload.theme),
        appearanceOverride: appearanceOverride === "Light" || appearanceOverride === "Dark" ? appearanceOverride : null,
        view: payload.view === "Welcome" ? "Welcome" : "Conversation",
    };
}

export function announceReady(expectedOrigin: string): void {
    if (window.parent === window) return;
    window.parent.postMessage(envelope("ready", {}), expectedOrigin);
}
