import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
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
    const fontSelectOptions = fontOptions.map((option) => ({ value: option.stack, label: option.label }));
    // A hand-written stack saved earlier is not in the curated list; offer it so the select can show it.
    const hasCuratedFont = fontSelectOptions.some((option) => option.value === previewTheme.fontFamily);

    return (
        <ThemeEditorSection title="Style" control={control} paths={SECTION_FIELDS.style} onReset={onReset}>
            <FormSelect
                control={control}
                name="radius"
                label="Radius"
                description="Rounds the corners inside the widget - message bubbles, code blocks, the composer and the prompt pills."
                options={RADIUS_OPTIONS}
                disabled={isSaving}
            />
            <FormSelect
                control={control}
                name="fontFamily"
                label="Font"
                options={
                    hasCuratedFont
                        ? fontSelectOptions
                        : [...fontSelectOptions, { value: previewTheme.fontFamily, label: "Custom" }]
                }
                disabled={isSaving}
            />
            <FormSelect
                control={control}
                name="fontSize"
                label="Font size"
                options={FONT_SIZE_OPTIONS}
                disabled={isSaving}
            />
            {previewTheme.fontSize === "Custom" && (
                <FormInput
                    control={control}
                    name="customFontSizeRem"
                    label="Custom font size (rem)"
                    type="number"
                    step="0.025"
                    min={MIN_CUSTOM_FONT_SIZE_REM}
                    max={MAX_CUSTOM_FONT_SIZE_REM}
                    placeholder="1"
                    description={`1 is the standard size; ${MIN_CUSTOM_FONT_SIZE_REM}-${MAX_CUSTOM_FONT_SIZE_REM}.`}
                    disabled={isSaving}
                />
            )}
        </ThemeEditorSection>
    );
}
