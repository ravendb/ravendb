import { describe, expect, it } from "vitest";
import { COLOR_PALETTES, presetColorsFor } from "@/pages/apps/channels/theme-editor/color-presets";

// Luminance is recomputed here rather than imported from the widget package. The dashboard has no
// import path into that package by design (an alias existed for exactly one consumer and was removed
// when that consumer went away), and resurrecting one for a test-only guard would be the wrong trade.
// Kept deliberately literal so it is obvious this is the WCAG 2.1 formula and not a variant.
const relativeLuminance = (hex: string) => {
    const channels = [1, 3, 5].map((offset) => parseInt(hex.slice(offset, offset + 2), 16) / 255);
    const [r, g, b] = channels.map((value) => (value <= 0.03928 ? value / 12.92 : ((value + 0.055) / 1.055) ** 2.4));
    return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};

const contrastRatio = (a: string, b: string) => {
    const [lighter, darker] = [relativeLuminance(a), relativeLuminance(b)].sort((x, y) => y - x);
    return (lighter + 0.05) / (darker + 0.05);
};

const SCHEMES = ["light", "dark"] as const;

describe("color presets", () => {
    it.each(COLOR_PALETTES.flatMap((palette) => SCHEMES.map((scheme) => [palette.name, scheme] as const)))(
        "%s %s uses six digit lower case hex",
        (name, scheme) => {
            const colors = COLOR_PALETTES.find((palette) => palette.name === name)![scheme];
            for (const value of Object.values(colors)) expect(value).toMatch(/^#[0-9a-f]{6}$/);
        },
    );

    // The floor WCAG 1.4.11 sets for a UI component against its background. The product's own default
    // accent sits at 2.61:1 on white and would fail this, which is the whole reason the guard exists:
    // a preset an operator picks from a curated list should not be the thing that fails accessibility.
    it.each(COLOR_PALETTES.flatMap((palette) => SCHEMES.map((scheme) => [palette.name, scheme] as const)))(
        "%s %s keeps its button readable as a control against its own background",
        (name, scheme) => {
            const { buttonColor, backgroundColor } = COLOR_PALETTES.find((palette) => palette.name === name)![scheme];
            expect(contrastRatio(buttonColor, backgroundColor)).toBeGreaterThanOrEqual(3);
        },
    );

    // A bubble is a tinted surface, not text, so it is meant to sit close to the background. It still
    // has to be perceptible as a shape, and it must not collide with the background outright.
    it.each(COLOR_PALETTES.flatMap((palette) => SCHEMES.map((scheme) => [palette.name, scheme] as const)))(
        "%s %s keeps its bubble distinct from its background",
        (name, scheme) => {
            const { messageColor, backgroundColor } = COLOR_PALETTES.find((palette) => palette.name === name)![scheme];
            expect(messageColor).not.toBe(backgroundColor);
            expect(contrastRatio(messageColor, backgroundColor)).toBeGreaterThan(1.05);
        },
    );

    it("offers one swatch per palette, in palette order", () => {
        expect(presetColorsFor("Light", "buttonColor")).toEqual(COLOR_PALETTES.map((p) => p.light.buttonColor));
        expect(presetColorsFor("Dark", "backgroundColor")).toEqual(COLOR_PALETTES.map((p) => p.dark.backgroundColor));
    });

    it("gives each field its own palette rather than one shared list", () => {
        const buttons = presetColorsFor("Light", "buttonColor");
        const backgrounds = presetColorsFor("Light", "backgroundColor");
        expect(buttons).not.toEqual(backgrounds);
    });
});
