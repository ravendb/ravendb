import { normalizeTheme } from "@/widget-config";
import type { ResolvedAppearance, WidgetTheme } from "@/widget-theme";

export const ENVELOPE_SOURCE = "raven-quill";
export const ENVELOPE_VERSION = 1;

export type PreviewThemePayload = {
    theme: Partial<WidgetTheme>;
    /** Lets the editor's light/dark toggle preview both without editing the saved appearance. */
    appearanceOverride?: ResolvedAppearance | null;
};

type Envelope<TType extends string, TPayload> = {
    source: typeof ENVELOPE_SOURCE;
    version: typeof ENVELOPE_VERSION;
    type: TType;
    payload: TPayload;
};

export type PreviewMessage = Envelope<"theme", PreviewThemePayload>;

/** What the widget acts on: the sender's partial theme merged over the defaults. */
export type ResolvedPreviewTheme = {
    theme: WidgetTheme;
    appearanceOverride: ResolvedAppearance | null;
};

export type WidgetReadyMessage = Envelope<"ready", Record<string, never>>;

export function envelope<TType extends string, TPayload>(type: TType, payload: TPayload): Envelope<TType, TPayload> {
    return { source: ENVELOPE_SOURCE, version: ENVELOPE_VERSION, type, payload };
}

function isEnvelope(value: unknown): value is Envelope<string, unknown> {
    if (typeof value !== "object" || value === null) return false;
    const candidate = value as Record<string, unknown>;
    return (
        candidate.source === ENVELOPE_SOURCE &&
        candidate.version === ENVELOPE_VERSION &&
        typeof candidate.type === "string"
    );
}

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
    };
}

export function announceReady(expectedOrigin: string): void {
    if (window.parent === window) return;
    window.parent.postMessage(envelope("ready", {}), expectedOrigin);
}
