import { useController } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { FormSlider } from "@/components/form/form-slider";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorFontFamilyField } from "@/pages/apps/channels/theme-editor/theme-editor-font-family-field";
import { ThemeEditorFontSizeField } from "@/pages/apps/channels/theme-editor/theme-editor-font-size-field";
import { ThemeEditorRadiusField } from "@/pages/apps/channels/theme-editor/theme-editor-radius-field";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import {
    FONT_SIZE_OPTIONS,
    MAX_CUSTOM_FONT_SIZE_REM,
    MIN_CUSTOM_FONT_SIZE_REM,
    RADIUS_OPTIONS,
} from "@/pages/apps/channels/web-widget-theme-schema";

type StyleSectionProps = ThemeSectionProps & {
    fontOptions: WidgetFontOption[];
    previewTheme: WidgetTheme;
};

export function StyleSection({ control, isSaving, onReset, fontOptions, previewTheme }: StyleSectionProps) {
    const customFontSize = useController({ control, name: "customFontSizeRem" });
    // A hand-written stack saved earlier is not in the curated list; offer it so the field can show it.
    const hasCuratedFont = fontOptions.some((option) => option.stack === previewTheme.fontFamily);
    const fontChoices = hasCuratedFont
        ? fontOptions
        : [...fontOptions, { stack: previewTheme.fontFamily, label: "Custom" }];

    return (
        <ThemeEditorSection title="Style" control={control} paths={SECTION_FIELDS.style} onReset={onReset}>
            <ThemeEditorRadiusField
                control={control}
                name="radius"
                label="Radius"
                options={RADIUS_OPTIONS}
                disabled={isSaving}
            />
            <ThemeEditorFontFamilyField
                control={control}
                name="fontFamily"
                label="Font"
                options={fontChoices}
                disabled={isSaving}
            />
            <ThemeEditorFontSizeField
                control={control}
                name="fontSize"
                label="Font size"
                options={FONT_SIZE_OPTIONS}
                // Custom starts from the standard size instead of from nothing: a slider's thumb has to
                // sit somewhere, so an unset value would only ever surface as an error on save.
                onValueChange={(next) => {
                    if (next === "Custom" && customFontSize.field.value === null) customFontSize.field.onChange(1);
                }}
                disabled={isSaving}
            />
            {previewTheme.fontSize === "Custom" && (
                <FormSlider
                    control={control}
                    name="customFontSizeRem"
                    label="Custom font size"
                    min={MIN_CUSTOM_FONT_SIZE_REM}
                    max={MAX_CUSTOM_FONT_SIZE_REM}
                    // A sixteenth of a rem: fine enough to tune by, coarse enough that the stops stay
                    // countable on the rail, and it lands exactly on 1 - the standard size.
                    step={0.0625}
                    fallback={1}
                    format={(value) => `${value.toFixed(3).replace(/0+$/, "").replace(/\.$/, "")} rem`}
                    disabled={isSaving}
                />
            )}
        </ThemeEditorSection>
    );
}
