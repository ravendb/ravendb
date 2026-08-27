import { InfoHint } from "@/components/data/info-hint";
import { FormInput } from "@/components/form/form-input";
import { FormSegmented } from "@/components/form/form-segmented";
import { FormStringList } from "@/components/form/form-string-list";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import { MAX_SUGGESTED_PROMPTS, SUGGESTED_PROMPTS_LAYOUT_OPTIONS } from "@/pages/apps/channels/web-widget-theme-schema";

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
                    description={`Starter questions on the welcome screen, up to ${MAX_SUGGESTED_PROMPTS}.`}
                    addButtonLabel="Add prompt"
                    emptyLabel="No suggested prompts."
                    defaultValue={{ value: "" }}
                    fieldName={(index) => `suggestedPrompts.${index}.value`}
                    maxItems={MAX_SUGGESTED_PROMPTS}
                    placeholder="Where is my order?"
                    disabled={isSaving}
                    sortable
                />
                <FormSegmented
                    control={control}
                    name="suggestedPromptsLayout"
                    label="Prompt layout"
                    hint="Stacked puts one prompt per row. Inline flows them as a wrapping row."
                    options={SUGGESTED_PROMPTS_LAYOUT_OPTIONS}
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
                label={
                    <span className="flex items-center gap-1.5">
                        Disclaimer
                        <InfoHint content="Sits as a small line under the composer. Leave it empty to hide it." />
                    </span>
                }
                placeholder="AI responses may be inaccurate."
                disabled={isSaving}
            />
        </ThemeEditorSection>
    );
}
