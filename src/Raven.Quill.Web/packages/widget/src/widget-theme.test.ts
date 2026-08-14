import { describe, expect, it } from "vitest";
import {
    contrastRatio,
    derivePalette,
    DEFAULT_ACCENT_COLOR,
    DEFAULT_THEME,
    isValidHexColor,
    resolveFontSizeRem,
    widgetThemeStyle,
    type ResolvedAppearance,
    type WidgetThemeColors,
} from "@/widget-theme";

const WCAG_AA_NORMAL_TEXT = 4.5;

// A sweep wide enough to catch a palette rule that only holds for mid-tone colors: saturated primaries,
// near-white, near-black, and the mid greys where the black/white contrast pick crosses over.
const COLORS = [
    "#000000",
    "#ffffff",
    "#808080",
    "#767676",
    "#8a8a8a",
    "#ff0000",
    "#00ff00",
    "#0000ff",
    "#ffff00",
    "#00ffff",
    "#ff00ff",
    "#2f6f4f",
    "#5b4bd6",
    "#f59e0b",
    "#1d4ed8",
    "#fde68a",
    "#0b1220",
];

// Operator backgrounds across the whole range: light, dark, tinted, and the awkward mid-tones where
// neither soft foreground clears AA.
const BACKGROUNDS = ["#ffffff", "#000000", "#f7f5f0", "#0b1220", "#1e293b", "#808080", "#fde68a"];

const APPEARANCES: ResolvedAppearance[] = ["Light", "Dark"];

function colorsFor(appearance: ResolvedAppearance, overrides: Partial<WidgetThemeColors> = {}): WidgetThemeColors {
    return { ...(appearance === "Dark" ? DEFAULT_THEME.dark : DEFAULT_THEME.light), ...overrides };
}

describe("isValidHexColor", () => {
    it("accepts 3- and 6-digit hex, in either case", () => {
        expect(isValidHexColor("#abc")).toBe(true);
        expect(isValidHexColor("#AABBCC")).toBe(true);
        expect(isValidHexColor("  #2f6f4f  ")).toBe(true);
    });

    it("rejects anything else", () => {
        for (const value of ["abc", "#ab", "#abcd", "#gggggg", "rgb(0,0,0)", "red", "", "#abcdef;"])
            expect(isValidHexColor(value)).toBe(false);
    });
});

describe("derivePalette", () => {
    it("falls back to the default accent when the button color is not a hex color", () => {
        expect(derivePalette(colorsFor("Light", { buttonColor: "not-a-color" }), "Light").accent).toBe(
            DEFAULT_ACCENT_COLOR,
        );
    });

    it("normalises the button color it echoes back as the accent", () => {
        expect(derivePalette(colorsFor("Light", { buttonColor: "  #2F6F4F " }), "Light").accent).toBe("#2f6f4f");
    });

    it("keeps body text at or above 4.5:1 against its background in both appearances", () => {
        for (const appearance of APPEARANCES) {
            const palette = derivePalette(colorsFor(appearance), appearance);
            expect(contrastRatio(palette.fg, palette.bg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.mutedFg, palette.bg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.fg, palette.surface)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.fg, palette.codeBg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
        }
    });

    it("keeps text on the accent at or above 4.5:1 for every button color", () => {
        for (const buttonColor of COLORS) {
            for (const appearance of APPEARANCES) {
                const palette = derivePalette(colorsFor(appearance, { buttonColor }), appearance);
                expect(
                    contrastRatio(palette.accentFg, palette.accent),
                    `accentFg on ${buttonColor}`,
                ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            }
        }
    });

    it("uses the message color for the user bubble and keeps it readable whatever the pick", () => {
        for (const messageColor of COLORS) {
            for (const appearance of APPEARANCES) {
                const palette = derivePalette(colorsFor(appearance, { messageColor }), appearance);
                expect(palette.userBubbleBg).toBe(messageColor);
                expect(
                    contrastRatio(palette.userBubbleFg, palette.userBubbleBg),
                    `user bubble on ${messageColor} / ${appearance}`,
                ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            }
        }
    });

    it("derives the user bubble from the button color and background when the message color is invalid", () => {
        for (const appearance of APPEARANCES) {
            const palette = derivePalette(colorsFor(appearance, { messageColor: "not-a-color" }), appearance);
            expect(isValidHexColor(palette.userBubbleBg)).toBe(true);
            expect(contrastRatio(palette.userBubbleFg, palette.userBubbleBg)).toBeGreaterThanOrEqual(
                WCAG_AA_NORMAL_TEXT,
            );
        }
    });

    it("derives light and dark from the same button color without collapsing them", () => {
        const light = derivePalette(colorsFor("Light"), "Light");
        const dark = derivePalette(colorsFor("Dark"), "Dark");
        expect(light.bg).not.toBe(dark.bg);
        expect(light.fg).not.toBe(dark.fg);
        expect(light.accent).toBe(dark.accent);
    });

    it("keeps the surface distinguishable from the background", () => {
        for (const appearance of APPEARANCES) {
            const palette = derivePalette(colorsFor(appearance), appearance);
            expect(palette.surface).not.toBe(palette.bg);
            expect(palette.border).not.toBe(palette.bg);
        }
    });

    it("uses a changed background whatever the appearance, and stays readable on it", () => {
        for (const backgroundColor of BACKGROUNDS) {
            for (const appearance of APPEARANCES) {
                const palette = derivePalette(colorsFor(appearance, { backgroundColor }), appearance);
                expect(palette.bg).toBe(backgroundColor);
                expect(
                    contrastRatio(palette.fg, palette.bg),
                    `fg on ${backgroundColor} / ${appearance}`,
                ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
                expect(
                    contrastRatio(palette.mutedFg, palette.bg),
                    `mutedFg on ${backgroundColor} / ${appearance}`,
                ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            }
        }
    });

    it("resolves the color scheme from the background, not the appearance", () => {
        expect(derivePalette(colorsFor("Light", { backgroundColor: "#0b1220" }), "Light").colorScheme).toBe("dark");
        expect(derivePalette(colorsFor("Dark", { backgroundColor: "#f7f5f0" }), "Dark").colorScheme).toBe("light");
    });

    it("falls back to the scheme background when the value is invalid", () => {
        const palette = derivePalette(colorsFor("Light", { backgroundColor: "not-a-color" }), "Light");
        expect(palette.bg).toBe(derivePalette(colorsFor("Light"), "Light").bg);
    });
});

describe("widgetThemeStyle", () => {
    it("sets color-scheme from the resolved appearance", () => {
        expect(widgetThemeStyle(DEFAULT_THEME, "Dark")["color-scheme"]).toBe("dark");
        expect(widgetThemeStyle(DEFAULT_THEME, "Light")["color-scheme"]).toBe("light");
    });

    it("derives every radius from the single named size", () => {
        const none = widgetThemeStyle({ ...DEFAULT_THEME, radius: "None" }, "Light");
        expect(none["--rq-radius"]).toBe("0px");
        expect(none["--rq-radius-sm"]).toBe("0px");
        expect(none["--rq-radius-pill"]).toBe("0px");

        const large = widgetThemeStyle({ ...DEFAULT_THEME, radius: "Large" }, "Light");
        expect(large["--rq-radius"]).toBe("18px");
        expect(large["--rq-radius-sm"]).toBe("10px");
        expect(large["--rq-radius-pill"]).toBe("9999px");
    });

    it("sets the logo radius independently of the widget radius", () => {
        expect(widgetThemeStyle(DEFAULT_THEME, "Light")["--rq-logo-radius"]).toBe("100vh");
        expect(widgetThemeStyle({ ...DEFAULT_THEME, logoRadius: "None" }, "Light")["--rq-logo-radius"]).toBe("0px");
        expect(widgetThemeStyle({ ...DEFAULT_THEME, radius: "None" }, "Light")["--rq-logo-radius"]).toBe("100vh");
    });

    it("paints from the colors of the resolved scheme", () => {
        const theme = {
            ...DEFAULT_THEME,
            light: { buttonColor: "#2f6f4f", messageColor: "#e0ece6", backgroundColor: "#f7f5f0" },
            dark: { buttonColor: "#1d4ed8", messageColor: "#16233f", backgroundColor: "#0d1117" },
        };
        const light = widgetThemeStyle(theme, "Light");
        expect(light["--rq-accent"]).toBe("#2f6f4f");
        expect(light["--rq-user-bubble-bg"]).toBe("#e0ece6");
        expect(light["--rq-bg"]).toBe("#f7f5f0");

        const dark = widgetThemeStyle(theme, "Dark");
        expect(dark["--rq-accent"]).toBe("#1d4ed8");
        expect(dark["--rq-user-bubble-bg"]).toBe("#16233f");
        expect(dark["color-scheme"]).toBe("dark");
    });

    it("sets a changed background on the root variable", () => {
        const style = widgetThemeStyle(
            { ...DEFAULT_THEME, light: { ...DEFAULT_THEME.light, backgroundColor: "#0b1220" } },
            "Light",
        );
        expect(style["--rq-bg"]).toBe("#0b1220");
        expect(style["color-scheme"]).toBe("dark");
    });
});

describe("resolveFontSizeRem", () => {
    it("maps the named sizes", () => {
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Small" })).toBe(0.875);
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Medium" })).toBe(1);
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Large" })).toBe(1.125);
    });

    it("uses the custom value only when fontSize is Custom, clamped to its bounds", () => {
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Custom", customFontSizeRem: 1.05 })).toBe(1.05);
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Custom", customFontSizeRem: 99 })).toBe(1.5);
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Custom", customFontSizeRem: null })).toBe(1);
        expect(resolveFontSizeRem({ ...DEFAULT_THEME, fontSize: "Small", customFontSizeRem: 1.4 })).toBe(0.875);
    });
});
