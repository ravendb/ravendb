import { describe, expect, it } from "vitest";
import { normalizeTheme, resolveMount } from "@/widget-config";

const LIVE_CONFIG = JSON.stringify({
    mode: "live",
    chatUrl: "/apps/shop/embed/tok/chat",
    theme: { headerTitle: "Order support" },
    history: [{ role: "user", content: "hi" }],
});

describe("resolveMount", () => {
    it("mounts live when the shell embedded a config block", () => {
        const mount = resolveMount(LIVE_CONFIG, "");

        expect(mount).toMatchObject({ mode: "live" });
        expect(mount.mode === "live" && mount.config.chatUrl).toBe("/apps/shop/embed/tok/chat");
    });

    // A live shell passes its own query string down to this document - `?appearance=` is a documented host
    // option - so a visitor appending `?mode=preview` must not get the canned demo transcript instead of
    // their own conversation.
    it("stays live when the URL asks for preview but a config block is present", () => {
        expect(resolveMount(LIVE_CONFIG, "?mode=preview&appearance=dark")).toMatchObject({ mode: "live" });
    });

    it("previews only when there is no config block to serve", () => {
        expect(resolveMount(null, "?mode=preview")).toEqual({ mode: "preview" });
    });

    it("reports a shell served without a usable config block", () => {
        expect(resolveMount(null, "")).toEqual({ mode: "unusable" });
        expect(resolveMount("", "")).toEqual({ mode: "unusable" });
        expect(resolveMount("{ not json", "")).toEqual({ mode: "unusable" });
        expect(resolveMount(JSON.stringify({ mode: "live" }), "")).toEqual({ mode: "unusable" });
    });

    // A malformed config is still a live shell: falling through to the preview would show a fabricated
    // conversation on the operator's own public origin.
    it("does not fall back to preview when a live config is malformed", () => {
        expect(resolveMount("{ not json", "?mode=preview")).toEqual({ mode: "unusable" });
    });
});

// A server that predates the option sends no `suggestedPromptsLayout` at all. Merging over the defaults has
// to leave the stacked layout the widget has always drawn, not `undefined`, which would fall through to the
// inline branch in `Greeting`.
describe("normalizeTheme", () => {
    it("stacks the prompts when the server omits the layout", () => {
        expect(normalizeTheme({ headerTitle: "Order support" }).suggestedPromptsLayout).toBe("Stacked");
    });

    it("keeps a layout the server did send", () => {
        expect(normalizeTheme({ suggestedPromptsLayout: "Inline" }).suggestedPromptsLayout).toBe("Inline");
    });
});
