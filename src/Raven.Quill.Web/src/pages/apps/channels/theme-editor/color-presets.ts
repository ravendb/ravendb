import type { WidgetThemeColors } from "@/api/generated/server-api";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";

/**
 * Curated colours offered in the picker, so a new channel is not six empty hex fields.
 *
 * Authored as whole palettes rather than three flat lists, because that is the unit a maintainer
 * reasons about: a palette's bubble is chosen against its own background, and its dark counterpart
 * against its own. The picker only ever needs one field's swatches, so the flat form is derived below.
 *
 * Every accent clears 3:1 against its palette's background, the WCAG 1.4.11 floor for a UI component.
 * That is deliberate and guarded by a test: the product's own default accent does not clear it, and
 * these should not inherit that.
 */
type Palette = {
    name: string;
    light: WidgetThemeColors;
    dark: WidgetThemeColors;
};

const PALETTES: readonly Palette[] = [
    {
        name: "Coral",
        light: { buttonColor: "#e2513a", messageColor: "#ffe9e4", backgroundColor: "#fffaf8" },
        dark: { buttonColor: "#ff8a70", messageColor: "#4a2a24", backgroundColor: "#14100f" },
    },
    {
        name: "Forest",
        light: { buttonColor: "#2f6f4f", messageColor: "#e3efe8", backgroundColor: "#f8faf8" },
        dark: { buttonColor: "#4cc38a", messageColor: "#16302a", backgroundColor: "#0d1411" },
    },
    {
        name: "Ocean",
        light: { buttonColor: "#1d63c9", messageColor: "#e3ecfb", backgroundColor: "#f7fafd" },
        dark: { buttonColor: "#6ba8ff", messageColor: "#16263f", backgroundColor: "#0c1017" },
    },
    {
        name: "Violet",
        light: { buttonColor: "#7345d6", messageColor: "#ece5fb", backgroundColor: "#faf8fe" },
        dark: { buttonColor: "#a98bff", messageColor: "#271b45", backgroundColor: "#100e17" },
    },
    {
        name: "Amber",
        light: { buttonColor: "#a86a00", messageColor: "#fdeecf", backgroundColor: "#fffcf5" },
        dark: { buttonColor: "#f0b23c", messageColor: "#3a2a0d", backgroundColor: "#16120a" },
    },
    {
        name: "Slate",
        light: { buttonColor: "#445064", messageColor: "#e8ecf2", backgroundColor: "#f8f9fb" },
        dark: { buttonColor: "#93a4bd", messageColor: "#212936", backgroundColor: "#0f1319" },
    },
];

/** Exported for the test that guards each palette's accent contrast. */
export const COLOR_PALETTES = PALETTES;

/** One field's curated swatches for one scheme, in palette order so the row reads consistently. */
export function presetColorsFor(scheme: PreviewAppearance, key: keyof WidgetThemeColors): readonly string[] {
    return PALETTES.map((palette) => (scheme === "Light" ? palette.light : palette.dark)[key]);
}
