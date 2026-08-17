import { describe, expect, it } from "vitest";
import { widgetThemeSchema, toFormData } from "@/pages/apps/channels/web-widget-theme-schema";
import type { WidgetTheme } from "@/api/generated/server-api";

const theme: WidgetTheme = {
    appearance: "Light",
    light: { buttonColor: "#5b4bd6", messageColor: "#ebe9fa", backgroundColor: "#ffffff" },
    dark: { buttonColor: "#5b4bd6", messageColor: "#201f45", backgroundColor: "#0d1117" },
    radius: "Medium",
    fontFamily: "system-ui, -apple-system, sans-serif",
    fontSize: "Medium",
    customFontSizeRem: null,
    logo: null,
    logoRadius: "Pill",
    headerTitle: "AI Assistant",
    headerSubtitle: null,
    showHeader: true,
    greetingTitle: null,
    greetingBody: null,
    suggestedPrompts: [],
    inputPlaceholder: "Ask a question...",
    disclaimer: null,
    customCss: null,
};

const parse = (overrides: Partial<ReturnType<typeof toFormData>>) =>
    widgetThemeSchema.safeParse({ ...toFormData(theme), ...overrides });

/** Rules that only apply while the option they belong to is on: the value behind a switched-off option is
 *  either invisible in the editor or dropped on save, so it must never block the form. */
describe("widgetThemeSchema", () => {
    it("requires a title while the header is shown", () => {
        const result = parse({ showHeader: true, headerTitle: "  " });
        expect(result.success).toBe(false);
        expect(result.error?.issues.map((issue) => issue.path.join("."))).toContain("headerTitle");
    });

    it("allows a blank title while the header is hidden", () => {
        expect(parse({ showHeader: false, headerTitle: "" }).success).toBe(true);
    });

    it("still bounds the title length while the header is hidden", () => {
        expect(parse({ showHeader: false, headerTitle: "x".repeat(61) }).success).toBe(false);
    });

    it("ignores blank rows when counting suggested prompts", () => {
        const prompts = ["a", "b", "c", "d", "  "].map((value) => ({ value }));
        expect(parse({ suggestedPrompts: prompts }).success).toBe(true);
    });

    it("rejects five non-blank prompts", () => {
        const prompts = ["a", "b", "c", "d", "e"].map((value) => ({ value }));
        expect(parse({ suggestedPrompts: prompts }).success).toBe(false);
    });

    it("ignores a stale custom font size while the size is named", () => {
        expect(parse({ fontSize: "Medium", customFontSizeRem: 9 }).success).toBe(true);
    });
});
