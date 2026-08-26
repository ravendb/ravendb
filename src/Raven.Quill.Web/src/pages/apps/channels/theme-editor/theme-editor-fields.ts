import type { Control } from "react-hook-form";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

/** Shared by every theme editor section so the inspector can render them uniformly. */
export type ThemeSectionProps = {
    control: Control<WidgetThemeFormData>;
    isSaving: boolean;
    onReset: (paths: readonly (keyof WidgetThemeFormData)[]) => void;
};

export const SCHEME_COLOR_FIELDS = {
    Light: ["lightButtonColor", "lightMessageColor", "lightBackgroundColor"],
    Dark: ["darkButtonColor", "darkMessageColor", "darkBackgroundColor"],
} as const satisfies Record<PreviewAppearance, readonly (keyof WidgetThemeFormData)[]>;

// Top-level keys only (keyof, not FieldPath): resetSection restores each path with a plain
// savedValues[path] lookup, which silently resolves to undefined for a nested path such as
// "suggestedPrompts.0.value". FieldPath would have permitted that and wiped the field.
export const SECTION_FIELDS = {
    colors: [...SCHEME_COLOR_FIELDS.Light, ...SCHEME_COLOR_FIELDS.Dark],
    style: ["radius", "fontFamily", "fontSize", "customFontSizeRem"],
    branding: ["showHeader", "logo", "logoRadius", "logoFit", "headerTitle", "headerSubtitle"],
    content: [
        "greetingTitle",
        "greetingBody",
        "suggestedPrompts",
        "suggestedPromptsLayout",
        "inputPlaceholder",
        "disclaimer",
    ],
    customCss: ["customCss"],
} as const satisfies Record<string, readonly (keyof WidgetThemeFormData)[]>;
