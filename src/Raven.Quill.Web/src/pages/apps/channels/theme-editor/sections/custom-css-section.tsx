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
                description="Loads after the widget's own styles, so it can cover what the options above miss, like scrollbars or one-off spacing."
            />
        </ThemeEditorSection>
    );
}
