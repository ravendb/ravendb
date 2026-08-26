import type { Control } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme, WidgetThemeColors } from "@/api/generated/server-api";
import { BrandingSection } from "@/pages/apps/channels/theme-editor/sections/branding-section";
import { ColorsSection } from "@/pages/apps/channels/theme-editor/sections/colors-section";
import { ContentSection } from "@/pages/apps/channels/theme-editor/sections/content-section";
import { CustomCssSection } from "@/pages/apps/channels/theme-editor/sections/custom-css-section";
import { StyleSection } from "@/pages/apps/channels/theme-editor/sections/style-section";
import { ThemeEditorAppearanceField } from "@/pages/apps/channels/theme-editor/theme-editor-appearance-field";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

type ThemeEditorInspectorProps = {
    control: Control<WidgetThemeFormData>;
    isSaving: boolean;
    onReset: (paths: readonly (keyof WidgetThemeFormData)[]) => void;
    fontOptions: WidgetFontOption[];
    previewTheme: WidgetTheme;
    previewAppearance: PreviewAppearance;
    onPreviewAppearanceChange: (next: PreviewAppearance) => void;
    onFocusWelcomeFields: () => void;
    savedColors: Record<PreviewAppearance, WidgetThemeColors>;
    defaultColors: Record<PreviewAppearance, WidgetThemeColors>;
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
    savedColors,
    defaultColors,
}: ThemeEditorInspectorProps) {
    return (
        // One panel, not a stack of cards: bg-card carries the whole column and the sections inside it
        // are told apart by dividers alone.
        // Bounded and independently scrolling only once the two-pane split is actually active
        // (@5xl/theme-editor, set on the form in theme-editor.tsx); below that it takes its natural
        // height so the page scrolls once instead of this column scrolling on its own inside a
        // height it doesn't have room for.
        <div className="shrink-0 border-b bg-card @5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1 @5xl/theme-editor:overflow-y-auto @5xl/theme-editor:border-r @5xl/theme-editor:border-b-0">
            <div className="divide-y">
                <div className="p-4">
                    <ThemeEditorAppearanceField
                        control={control}
                        name="appearance"
                        label="Default color scheme"
                        // The cards carry what each scheme means, so all this line still owes the reader is
                        // that the choice is a default rather than a lock, and where the override is documented -
                        // little enough to sit behind the label's help icon instead of under the cards.
                        hint="The embedding page can override this - see “Embed on your own site” on the channel page."
                        light={previewTheme.light}
                        dark={previewTheme.dark}
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
                    savedColors={savedColors}
                    defaultColors={defaultColors}
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
