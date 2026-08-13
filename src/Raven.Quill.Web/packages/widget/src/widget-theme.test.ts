import { describe, expect, it } from "vitest";
import {
    clampRadius,
    contrastRatio,
    derivePalette,
    DEFAULT_ACCENT_COLOR,
    DEFAULT_THEME,
    isValidAccentColor,
    widgetThemeStyle,
    type ResolvedAppearance,
} from "@/widget-theme";

const WCAG_AA_NORMAL_TEXT = 4.5;

// A sweep wide enough to catch a palette rule that only holds for mid-tone accents: saturated primaries,
// near-white, near-black, and the mid greys where the black/white contrast pick crosses over.
const ACCENTS = [
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

const APPEARANCES: ResolvedAppearance[] = ["Light", "Dark"];

describe("isValidAccentColor", () => {
    it("accepts 3- and 6-digit hex, in either case", () => {
        expect(isValidAccentColor("#abc")).toBe(true);
        expect(isValidAccentColor("#AABBCC")).toBe(true);
        expect(isValidAccentColor("  #2f6f4f  ")).toBe(true);
    });

    it("rejects anything else", () => {
        for (const value of ["abc", "#ab", "#abcd", "#gggggg", "rgb(0,0,0)", "red", "", "#abcdef;"])
            expect(isValidAccentColor(value)).toBe(false);
    });
});

describe("derivePalette", () => {
    it("falls back to the default accent when the input is not a hex colour", () => {
        expect(derivePalette("not-a-colour", "Light").accent).toBe(DEFAULT_ACCENT_COLOR);
    });

    it("normalises the accent it echoes back", () => {
        expect(derivePalette("  #2F6F4F ", "Light").accent).toBe("#2f6f4f");
    });

    it("keeps body text at or above 4.5:1 against its background in both appearances", () => {
        for (const appearance of APPEARANCES) {
            const palette = derivePalette(DEFAULT_ACCENT_COLOR, appearance);
            expect(contrastRatio(palette.fg, palette.bg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.mutedFg, palette.bg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.fg, palette.surface)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            expect(contrastRatio(palette.fg, palette.codeBg)).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
        }
    });

    it("keeps text on the accent at or above 4.5:1 for every accent", () => {
        for (const accent of ACCENTS) {
            for (const appearance of APPEARANCES) {
                const palette = derivePalette(accent, appearance);
                expect(contrastRatio(palette.accentFg, palette.accent), `accentFg on ${accent}`).toBeGreaterThanOrEqual(
                    WCAG_AA_NORMAL_TEXT,
                );
            }
        }
    });

    it("keeps the user bubble readable for every accent", () => {
        for (const accent of ACCENTS) {
            for (const appearance of APPEARANCES) {
                const palette = derivePalette(accent, appearance);
                expect(
                    contrastRatio(palette.userBubbleFg, palette.userBubbleBg),
                    `user bubble on ${accent} / ${appearance}`,
                ).toBeGreaterThanOrEqual(WCAG_AA_NORMAL_TEXT);
            }
        }
    });

    it("derives light and dark from the same accent without collapsing them", () => {
        const light = derivePalette(DEFAULT_ACCENT_COLOR, "Light");
        const dark = derivePalette(DEFAULT_ACCENT_COLOR, "Dark");
        expect(light.bg).not.toBe(dark.bg);
        expect(light.fg).not.toBe(dark.fg);
        expect(light.accent).toBe(dark.accent);
    });

    it("keeps the surface distinguishable from the background", () => {
        for (const appearance of APPEARANCES) {
            const palette = derivePalette(DEFAULT_ACCENT_COLOR, appearance);
            expect(palette.surface).not.toBe(palette.bg);
            expect(palette.border).not.toBe(palette.bg);
        }
    });
});

describe("clampRadius", () => {
    it("clamps to 0-24 and rounds", () => {
        expect(clampRadius(-5)).toBe(0);
        expect(clampRadius(99)).toBe(24);
        expect(clampRadius(11.6)).toBe(12);
    });
});

describe("widgetThemeStyle", () => {
    it("sets color-scheme from the resolved appearance", () => {
        expect(widgetThemeStyle(DEFAULT_THEME, "Dark")["color-scheme"]).toBe("dark");
        expect(widgetThemeStyle(DEFAULT_THEME, "Light")["color-scheme"]).toBe("light");
    });

    it("derives every radius from the single radius knob", () => {
        const style = widgetThemeStyle({ ...DEFAULT_THEME, radius: 16 }, "Light");
        expect(style["--rq-radius"]).toBe("16px");
        expect(style["--rq-radius-sm"]).toBe("8px");
        expect(style["--rq-radius-pill"]).toBe("28px");
    });

    it("clamps an out-of-range radius rather than emitting it", () => {
        expect(widgetThemeStyle({ ...DEFAULT_THEME, radius: 999 }, "Light")["--rq-radius"]).toBe("24px");
    });

    it("changes spacing with density", () => {
        const comfortable = widgetThemeStyle({ ...DEFAULT_THEME, density: "Comfortable" }, "Light");
        const compact = widgetThemeStyle({ ...DEFAULT_THEME, density: "Compact" }, "Light");
        expect(comfortable["--rq-gap"]).not.toBe(compact["--rq-gap"]);
    });
});
