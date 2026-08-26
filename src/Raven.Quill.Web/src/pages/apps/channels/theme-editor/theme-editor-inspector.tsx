import type { Control, FieldPath } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { InlineCode } from "@/components/data/inline-code";
import { FormToggleGroup } from "@/components/form/form-toggle-group";
import { BrandingSection } from "@/pages/apps/channels/theme-editor/sections/branding-section";
import { ColorsSection } from "@/pages/apps/channels/theme-editor/sections/colors-section";
import { ContentSection } from "@/pages/apps/channels/theme-editor/sections/content-section";
import { CustomCssSection } from "@/pages/apps/channels/theme-editor/sections/custom-css-section";
import { StyleSection } from "@/pages/apps/channels/theme-editor/sections/style-section";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

const APPEARANCE_OPTIONS = [
    { value: "Light", label: "Light" },
    { value: "Dark", label: "Dark" },
    { value: "System", label: "System" },
] as const;

type ThemeEditorInspectorProps = {
    control: Control<WidgetThemeFormData>;
    isSaving: boolean;
    onReset: (paths: readonly FieldPath<WidgetThemeFormData>[]) => void;
    fontOptions: WidgetFontOption[];
    previewTheme: WidgetTheme;
    previewAppearance: PreviewAppearance;
    onPreviewAppearanceChange: (next: PreviewAppearance) => void;
    onFocusWelcomeFields: () => void;
};

export function ThemeEditorInspector({
    control,
    isSaving,
    onReset,
    fontOptions,
    previewTheme,
    previewAppearance,
    onPreviewAppearanceChange,
    onFocusWelcomeFields,
}: ThemeEditorInspectorProps) {
    return (
        <div className="min-h-0 flex-1 overflow-y-auto p-4">
            <div className="grid gap-4">
                <div className="rounded-md border bg-card p-4">
                    <FormToggleGroup
                        control={control}
                        name="appearance"
                        label="Default color scheme"
                        description={
                            <span>
                                System follows the visitor's own preference. The embedding page can override this with{" "}
                                <InlineCode className="whitespace-nowrap">?appearance=dark|light|system</InlineCode> on
                                the embed URL or an appearance message - see “Embed on your own site” on the channel
                                page.
                            </span>
                        }
                        options={APPEARANCE_OPTIONS}
                        canDeselect={false}
                        onValueChange={(next) => {
                            if (next === "Light" || next === "Dark") onPreviewAppearanceChange(next);
                        }}
                        disabled={isSaving}
                    />
                </div>

                <ColorsSection
                    control={control}
                    isSaving={isSaving}
                    onReset={onReset}
                    previewAppearance={previewAppearance}
                    onPreviewAppearanceChange={onPreviewAppearanceChange}
                />

                <StyleSection
                    control={control}
                    isSaving={isSaving}
                    onReset={onReset}
                    fontOptions={fontOptions}
                    previewTheme={previewTheme}
                />

                <BrandingSection control={control} isSaving={isSaving} onReset={onReset} previewTheme={previewTheme} />

                <ContentSection
                    control={control}
                    isSaving={isSaving}
                    onReset={onReset}
                    onFocusWelcomeFields={onFocusWelcomeFields}
                />

                <CustomCssSection control={control} isSaving={isSaving} onReset={onReset} />
            </div>
        </div>
    );
}
