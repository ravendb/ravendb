import { FormColorPicker } from "@/components/form/form-color-picker";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";

type ColorsSectionProps = ThemeSectionProps & {
    previewAppearance: PreviewAppearance;
    onPreviewAppearanceChange: (next: PreviewAppearance) => void;
};

export function ColorsSection({
    control,
    isSaving,
    onReset,
    previewAppearance,
    onPreviewAppearanceChange,
}: ColorsSectionProps) {
    const colorFields = (scheme: PreviewAppearance) => (
        <>
            <FormColorPicker
                control={control}
                name={scheme === "Light" ? "lightButtonColor" : "darkButtonColor"}
                label="Button color"
                description="Buttons, links and highlights."
                disabled={isSaving}
            />
            <FormColorPicker
                control={control}
                name={scheme === "Light" ? "lightMessageColor" : "darkMessageColor"}
                label="Message color"
                description="The visitor's message bubbles."
                disabled={isSaving}
            />
            <FormColorPicker
                control={control}
                name={scheme === "Light" ? "lightBackgroundColor" : "darkBackgroundColor"}
                label="Background color"
                disabled={isSaving}
            />
        </>
    );

    return (
        <ThemeEditorSection
            title="Colors"
            control={control}
            paths={SECTION_FIELDS.colors}
            defaultOpen
            onReset={onReset}
        >
            <p className="text-sm text-muted-foreground">
                Each scheme keeps its own colors. Every other option applies to both.
            </p>
            <Tabs
                value={previewAppearance}
                onValueChange={(next) => onPreviewAppearanceChange(next as PreviewAppearance)}
            >
                <TabsList>
                    <TabsTrigger value="Light">Light</TabsTrigger>
                    <TabsTrigger value="Dark">Dark</TabsTrigger>
                </TabsList>
                <TabsContent value="Light" className="mt-3 grid gap-4">
                    {colorFields("Light")}
                </TabsContent>
                <TabsContent value="Dark" className="mt-3 grid gap-4">
                    {colorFields("Dark")}
                </TabsContent>
            </Tabs>
        </ThemeEditorSection>
    );
}
