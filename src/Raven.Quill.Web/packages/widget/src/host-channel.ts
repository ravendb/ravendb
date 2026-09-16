import { envelope, isEnvelope, type Envelope } from "@/envelope";
import type { WidgetAppearance } from "@/widget-theme";

/** What a live embed tells the page that frames it. Without this a host could only guess: it had no way
 *  to know the widget had finished booting, and no way to learn that the link died — which, with a
 *  one-hour default TTL, is the normal end state of every embed rather than an edge case.
 *
 *  Mirrored in C# by `Raven.Quill.Embed.WidgetNotice`, so the same `expired` arrives whether the link
 *  died before the page was served or during the conversation.
 *
 *  There is deliberately no `resize`: the widget is a full-height panel that scrolls internally, so it
 *  always measures exactly as tall as the iframe it sits in and the message would only echo the host's
 *  own layout back at it. Size the frame with CSS. */
export type HostMessage =
    | Envelope<"ready", Record<string, never>>
    /** Terminal: the host should mint a new link and swap the iframe's src to resume. */
    | Envelope<"expired", { reason: "expired" | "limit" }>
    | Envelope<"error", { message: string }>;

const TARGET_ORIGIN = "*";

/** Every message is deliberately data-free — a type, and for an error a fixed English string — so no
 *  target origin has to be known. A framed navigation carries no `Origin` header, so a live embed cannot
 *  learn its parent's origin, and restricting the target would just mean sending nothing. */
function post(message: HostMessage): void {
    if (window.parent === window) return;
    try {
        window.parent.postMessage(message, TARGET_ORIGIN);
    } catch {
        // a parent that refuses the message is the host's problem, not the visitor's
    }
}

export function announceHostReady(): void {
    post(envelope("ready", {}));
}

export function announceHostExpired(reason: "expired" | "limit"): void {
    post(envelope("expired", { reason }));
}

export function announceHostError(message: string): void {
    post(envelope("error", { message }));
}

/** The one message a host may send the other way: a page with its own light/dark toggle keeps the widget
 *  in step by posting `appearance`. `System` hands control back to the visitor's OS preference. */
export type HostAppearanceMessage = Envelope<"appearance", { appearance: WidgetAppearance }>;

/** No origin check, deliberately: a framed widget cannot learn its parent's origin (see above), and the
 *  message can only pick between the palettes the operator already allows — it carries no data out. */
export function readHostAppearance(event: MessageEvent): WidgetAppearance | null {
    if (isEnvelope(event.data) === false || event.data.type !== "appearance") return null;

    const appearance = (event.data.payload as { appearance?: unknown } | null)?.appearance;
    return appearance === "Light" || appearance === "Dark" || appearance === "System" ? appearance : null;
}
