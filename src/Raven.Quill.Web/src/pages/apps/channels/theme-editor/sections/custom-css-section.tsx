import { FormAceEditor } from "@/components/form/form-ace-editor";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";

export function CustomCssSection({ control, isSaving, onReset }: ThemeSectionProps) {
    return (
        <ThemeEditorSection title="Custom CSS" control={control} paths={SECTION_FIELDS.customCss} onReset={onReset}>
            <FormAceEditor
                control={control}
                name="customCss"
                mode="css"
                height="220px"
                disabled={isSaving}
                description="Appended after the widget's own styles, for anything the options above don't cover — scrollbars, spacing, one-off tweaks."
            />
        </ThemeEditorSection>
    );
}
