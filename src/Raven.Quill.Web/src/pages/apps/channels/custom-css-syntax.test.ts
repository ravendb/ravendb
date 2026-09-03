import { describe, expect, it } from "vitest";
import { findCssSyntaxError } from "@/pages/apps/channels/custom-css-syntax";
import { widgetThemeSchema, type WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

function validFormData(customCss: string): WidgetThemeFormData {
    return {
        appearance: "Light",
        lightButtonColor: "#2f6f4f",
        lightMessageColor: "#ffefec",
        lightBackgroundColor: "#ffffff",
        darkButtonColor: "#2f6f4f",
        darkMessageColor: "#2b2b3c",
        darkBackgroundColor: "#111114",
        radius: "Medium",
        fontFamily: "Inter, sans-serif",
        fontSize: "Medium",
        customFontSizeRem: null,
        logo: "",
        logoRadius: "Medium",
        headerTitle: "AI Assistant",
        headerSubtitle: null,
        showHeader: true,
        greetingTitle: null,
        greetingBody: null,
        suggestedPrompts: [],
        inputPlaceholder: "Ask a question...",
        disclaimer: null,
        customCss,
    };
}

describe("findCssSyntaxError", () => {
    it("accepts an empty value", () => {
        expect(findCssSyntaxError("")).toBeUndefined();
        expect(findCssSyntaxError("\n  \n")).toBeUndefined();
    });

    it("accepts nesting, at-rules and custom properties", () => {
        const css = `
            :root {
                --gap: 8px;
            }

            @container (min-width: 200px) {
                .quill-message {
                    padding: var(--gap);

                    &:hover {
                        opacity: 0.9;
                    }
                }
            }
        `;

        expect(findCssSyntaxError(css)).toBeUndefined();
    });

    it("accepts braces and quotes inside strings and comments", () => {
        const css = `
            /* a comment with { and " in it */
            .quill-message::after {
                content: "}";
                background: url("data:image/png;base64,AA==");
                font-family: 'It\\'s Fine', sans-serif;
            }
        `;

        expect(findCssSyntaxError(css)).toBeUndefined();
    });

    it("reports an unclosed block", () => {
        expect(findCssSyntaxError(".a {\n  color: red;\n")).toBe("Unclosed block on line 1");
    });

    it("reports an unexpected closing brace", () => {
        expect(findCssSyntaxError(".a { color: red; }\n}\n")).toBe("Unexpected } on line 2");
    });

    it("reports an unclosed comment", () => {
        expect(findCssSyntaxError(".a { color: red; }\n/* trailing\n")).toBe("Unclosed comment on line 2");
    });

    it("reports an unclosed string", () => {
        expect(findCssSyntaxError('.a::after {\n  content: "oops;\n}\n')).toBe("Unclosed string on line 2");
    });

    it("reports a declaration missing its colon", () => {
        expect(findCssSyntaxError(".a {\n  color red;\n}\n")).toBe("Unknown word color on line 2");
    });
});

describe("widgetThemeSchema custom CSS", () => {
    it("accepts valid CSS", () => {
        expect(widgetThemeSchema.safeParse(validFormData(".a { color: red; }")).success).toBe(true);
    });

    it("rejects a broken rule, with the error on the customCss field", () => {
        const result = widgetThemeSchema.safeParse(validFormData(".a {\n  color: red;\n"));

        expect(result.success).toBe(false);
        expect(result.error?.issues).toEqual([
            expect.objectContaining({ path: ["customCss"], message: "Unclosed block on line 1" }),
        ]);
    });
});
