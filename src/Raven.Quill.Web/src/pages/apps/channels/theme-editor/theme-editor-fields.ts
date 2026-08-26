import type { Control, FieldPath } from "react-hook-form";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

/** Shared by every theme editor section so the inspector can render them uniformly. */
export type ThemeSectionProps = {
    control: Control<WidgetThemeFormData>;
    isSaving: boolean;
    onReset: (paths: readonly FieldPath<WidgetThemeFormData>[]) => void;
};

export const SCHEME_COLOR_FIELDS = {
    Light: ["lightButtonColor", "lightMessageColor", "lightBackgroundColor"],
    Dark: ["darkButtonColor", "darkMessageColor", "darkBackgroundColor"],
} as const satisfies Record<PreviewAppearance, readonly (keyof WidgetThemeFormData)[]>;

export const SECTION_FIELDS = {
    colors: [...SCHEME_COLOR_FIELDS.Light, ...SCHEME_COLOR_FIELDS.Dark],
    style: ["radius", "fontFamily", "fontSize", "customFontSizeRem"],
    branding: ["showHeader", "logo", "logoRadius", "headerTitle", "headerSubtitle"],
    content: ["greetingTitle", "greetingBody", "suggestedPrompts", "inputPlaceholder", "disclaimer"],
    customCss: ["customCss"],
} as const satisfies Record<string, readonly FieldPath<WidgetThemeFormData>[]>;
