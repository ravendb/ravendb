import { describe, expect, it } from "vitest";
import {
    MAX_HEADER_TITLE_LENGTH,
    MAX_SUGGESTED_PROMPTS,
    widgetThemeSchema,
    toFormData,
    toPreviewTheme,
    toWidgetTheme,
} from "@/pages/apps/channels/web-widget-theme-schema";
import type { WidgetTheme } from "@/api/generated/server-api";

const theme: WidgetTheme = {
    appearance: "Light",
    light: { buttonColor: "#ff775f", messageColor: "#ffefec", backgroundColor: "#ffffff" },
    dark: { buttonColor: "#ff775f", messageColor: "#472928", backgroundColor: "#0d1117" },
    radius: "Medium",
    fontFamily: "system-ui, -apple-system, sans-serif",
    fontSize: "Medium",
    customFontSizeRem: null,
    logo: null,
    logoRadius: "Pill",
    logoFit: "Contain",
    headerTitle: "AI Assistant",
    headerSubtitle: null,
    showHeader: true,
    greetingTitle: null,
    greetingBody: null,
    suggestedPrompts: [],
    suggestedPromptsLayout: "Stacked",
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

describe("suggestedPromptsLayout", () => {
    it("round-trips through the form", () => {
        const form = toFormData({ ...theme, suggestedPromptsLayout: "Inline" });
        expect(form.suggestedPromptsLayout).toBe("Inline");

        const parsed = widgetThemeSchema.parse(form);
        expect(toWidgetTheme(parsed).suggestedPromptsLayout).toBe("Inline");
    });

    it("rejects a layout that is neither value", () => {
        expect(parse({ suggestedPromptsLayout: "Sideways" as never }).success).toBe(false);
    });

    // The preview renders whatever is in the form right now, including a value react-hook-form has not
    // registered yet, so an unrecognised one has to fall back rather than reach the widget.
    it("previews the form value and falls back to the saved one", () => {
        expect(toPreviewTheme({ suggestedPromptsLayout: "Inline" }, theme).suggestedPromptsLayout).toBe("Inline");
        expect(toPreviewTheme({}, theme).suggestedPromptsLayout).toBe("Stacked");
        expect(toPreviewTheme({ suggestedPromptsLayout: "Sideways" as never }, theme).suggestedPromptsLayout).toBe(
            "Stacked",
        );
    });
});

/** A theme stored before `logoFit` and `suggestedPromptsLayout` shipped comes back without them, and the
 *  generated type does not admit that, so nothing coalesces them on the way into the form. */
describe("fields missing from a stored theme", () => {
    const stored = { ...theme } as WidgetTheme;
    delete (stored as Partial<WidgetTheme>).logoFit;
    delete (stored as Partial<WidgetTheme>).suggestedPromptsLayout;

    it("defaults them in the form rather than failing validation", () => {
        const values = toFormData(stored);
        expect(values.logoFit).toBe("Contain");
        expect(values.suggestedPromptsLayout).toBe("Stacked");
        expect(widgetThemeSchema.safeParse(values).success).toBe(true);
    });

    it("defaults them in the preview, which falls back to that same theme", () => {
        const preview = toPreviewTheme({}, stored);
        expect(preview.logoFit).toBe("Contain");
        expect(preview.suggestedPromptsLayout).toBe("Stacked");
    });

    it("saves them as the defaults", () => {
        const output = widgetThemeSchema.parse(toFormData(stored));
        expect(toWidgetTheme(output).logoFit).toBe("Contain");
        expect(toWidgetTheme(output).suggestedPromptsLayout).toBe("Stacked");
    });
});
