import type { WidgetTheme } from "@/api/generated/server-api";
import { FormImagePicker } from "@/components/form/form-image-picker";
import { FormSegmented } from "@/components/form/form-segmented";
import { FormInput } from "@/components/form/form-input";
import { FormSwitch } from "@/components/form/form-switch";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorRadiusField } from "@/pages/apps/channels/theme-editor/theme-editor-radius-field";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import { LOGO_FIT_OPTIONS, LOGO_RADIUS_OPTIONS } from "@/pages/apps/channels/web-widget-theme-schema";

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
                        description="Shown in the header next to the title."
                        hint="PNG, JPEG or WebP, resized down to 128px."
                        disabled={isSaving}
                    />
                    {/* Fit and radius only describe how a logo is drawn, so an empty slot has nothing to
                        answer for and they stay out of the panel until one is picked. */}
                    {previewTheme.logo && (
                        <>
                            <FormSegmented
                                control={control}
                                name="logoFit"
                                label="Logo fit"
                                hint="Contain fits the whole image inside the square. Cover fills the square and crops the overflow."
                                options={LOGO_FIT_OPTIONS}
                                disabled={isSaving}
                            />
                            <ThemeEditorRadiusField
                                control={control}
                                name="logoRadius"
                                label="Logo radius"
                                options={LOGO_RADIUS_OPTIONS}
                                disabled={isSaving}
                            />
                        </>
                    )}
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
