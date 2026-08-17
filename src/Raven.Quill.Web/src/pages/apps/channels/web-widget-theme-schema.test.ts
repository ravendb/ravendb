import { describe, expect, it } from "vitest";
import {
    MAX_HEADER_TITLE_LENGTH,
    MAX_SUGGESTED_PROMPTS,
    widgetThemeSchema,
    toFormData,
} from "@/pages/apps/channels/web-widget-theme-schema";
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

const promptRows = (count: number) => Array.from({ length: count }, (_, index) => ({ value: `prompt ${index + 1}` }));

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
        const overLongTitle = "x".repeat(MAX_HEADER_TITLE_LENGTH + 1);
        expect(parse({ showHeader: false, headerTitle: overLongTitle }).success).toBe(false);
    });

    // Derived from the limit rather than written out, so raising it does not silently turn an
    // "over the limit" case into an "at the limit" one.
    it("ignores blank rows when counting suggested prompts", () => {
        expect(parse({ suggestedPrompts: [...promptRows(MAX_SUGGESTED_PROMPTS), { value: "  " }] }).success).toBe(true);
    });

    it("rejects one prompt more than the limit allows", () => {
        expect(parse({ suggestedPrompts: promptRows(MAX_SUGGESTED_PROMPTS + 1) }).success).toBe(false);
    });

    it("ignores a stale custom font size while the size is named", () => {
        expect(parse({ fontSize: "Medium", customFontSizeRem: 9 }).success).toBe(true);
    });
});
