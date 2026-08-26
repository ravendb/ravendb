import type { WidgetTheme } from "@/api/generated/server-api";
import { FormImagePicker } from "@/components/form/form-image-picker";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormSwitch } from "@/components/form/form-switch";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import { LOGO_RADIUS_OPTIONS } from "@/pages/apps/channels/web-widget-theme-schema";

type BrandingSectionProps = ThemeSectionProps & {
    previewTheme: WidgetTheme;
};

export function BrandingSection({ control, isSaving, onReset, previewTheme }: BrandingSectionProps) {
    return (
        <ThemeEditorSection title="Branding" control={control} paths={SECTION_FIELDS.branding} onReset={onReset}>
            <FormSwitch control={control} name="showHeader" label="Show the header" disabled={isSaving} />
            {previewTheme.showHeader && (
                <>
                    <FormImagePicker
                        control={control}
                        name="logo"
                        label="Logo"
                        description="Shown in the header next to the title. Downscaled to 128px and stored with the theme."
                        disabled={isSaving}
                    />
                    <FormSelect
                        control={control}
                        name="logoRadius"
                        label="Logo radius"
                        description="Rounds only the logo. Pill crops a square logo to a circle."
                        options={LOGO_RADIUS_OPTIONS}
                        disabled={isSaving}
                    />
                    <FormInput
                        control={control}
                        name="headerTitle"
                        label="Title"
                        placeholder="AI Assistant"
                        disabled={isSaving}
                    />
                    <FormInput
                        control={control}
                        name="headerSubtitle"
                        label="Subtitle"
                        placeholder="Ask me anything"
                        disabled={isSaving}
                    />
                </>
            )}
        </ThemeEditorSection>
    );
}
