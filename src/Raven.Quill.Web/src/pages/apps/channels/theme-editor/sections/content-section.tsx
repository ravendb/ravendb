import { FormInput } from "@/components/form/form-input";
import { FormStringList } from "@/components/form/form-string-list";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import { MAX_SUGGESTED_PROMPTS } from "@/pages/apps/channels/web-widget-theme-schema";

type ContentSectionProps = ThemeSectionProps & {
    onFocusWelcomeFields: () => void;
};

export function ContentSection({ control, isSaving, onReset, onFocusWelcomeFields }: ContentSectionProps) {
    return (
        <ThemeEditorSection title="Content" control={control} paths={SECTION_FIELDS.content} onReset={onReset}>
            {/* These render only on the welcome screen, so editing them steers the preview there. */}
            <div className="grid gap-4" onFocusCapture={onFocusWelcomeFields}>
                <FormInput
                    control={control}
                    name="greetingTitle"
                    label="Greeting title"
                    placeholder="How can I help?"
                    disabled={isSaving}
                />
                <FormInput
                    control={control}
                    name="greetingBody"
                    label="Greeting body"
                    placeholder="Ask a question and I'll do my best to answer it."
                    disabled={isSaving}
                />
                <FormStringList
                    control={control}
                    name="suggestedPrompts"
                    label="Suggested prompts"
                    description={`Offered on the welcome screen. Up to ${MAX_SUGGESTED_PROMPTS}.`}
                    addButtonLabel="Add prompt"
                    emptyLabel="No suggested prompts."
                    defaultValue={{ value: "" }}
                    fieldName={(index) => `suggestedPrompts.${index}.value`}
                    placeholder="Where is my order?"
                    disabled={isSaving}
                />
            </div>
            <FormInput
                control={control}
                name="inputPlaceholder"
                label="Input placeholder"
                placeholder="Ask a question..."
                disabled={isSaving}
            />
            <FormInput
                control={control}
                name="disclaimer"
                label="Disclaimer"
                placeholder="AI responses may be inaccurate."
                description="Shown as a small line under the composer. Left blank, nothing is shown."
                disabled={isSaving}
            />
        </ThemeEditorSection>
    );
}
