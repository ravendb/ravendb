import { presetColorsFor } from "@/pages/apps/channels/theme-editor/color-presets";
import { SECTION_FIELDS, type ThemeSectionProps } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorColorRow } from "@/pages/apps/channels/theme-editor/theme-editor-color-row";
import { ThemeEditorSection } from "@/pages/apps/channels/theme-editor/theme-editor-section";
import { cn } from "@/lib/utils";
import type { WidgetThemeColors } from "@/api/generated/server-api";
import type { PreviewAppearance } from "@/pages/apps/channels/web-widget-theme-preview";
import type { WidgetThemeFormData } from "@/pages/apps/channels/web-widget-theme-schema";

type ColorsSectionProps = ThemeSectionProps & {
    previewAppearance: PreviewAppearance;
    onPreviewAppearanceChange: (next: PreviewAppearance) => void;
    /** The colours currently persisted for this channel, offered as one-click presets. */
    savedColors: Record<PreviewAppearance, WidgetThemeColors>;
    /**
     * The resolved app default (the same value behind this screen's "Follow app default" button),
     * offered as the other one-click preset. On the app-default editor this is the theme being edited,
     * so both anchors coincide and the dedup below leaves a single swatch: that page's way back to the
     * built-in is its "Reset to built-in default" button, not this row.
     */
    defaultColors: Record<PreviewAppearance, WidgetThemeColors>;
};

const SCHEMES: readonly PreviewAppearance[] = ["Light", "Dark"];

// `field` is derived from `key` below rather than declared alongside it, so the form path and the
// `WidgetThemeColors` key can never disagree.
const COLORS = [
    { label: "Button", key: "buttonColor" },
    { label: "Message", key: "messageColor" },
    { label: "Background", key: "backgroundColor" },
] as const satisfies readonly { label: string; key: keyof WidgetThemeColors }[];

const capitalize = (text: string) => text.charAt(0).toUpperCase() + text.slice(1);

export function ColorsSection({
    control,
    isSaving,
    onReset,
    previewAppearance,
    onPreviewAppearanceChange,
    savedColors,
    defaultColors,
}: ColorsSectionProps) {
    // Two anchors first, then the curated palettes: the anchors are where this channel came from, so
    // they stay leftmost where the eye lands. Deduplicated, because a channel that has never been
    // customised has the default saved already, and offering the same swatch twice reads like a bug.
    // Comparison is case-insensitive so a saved "#FF775F" doesn't slip past a default "#ff775f" as a
    // second, visually identical swatch, and it also collapses an anchor that matches a palette.
    const presetsFor = (key: keyof WidgetThemeColors) => {
        const seen = new Set<string>();
        return [
            defaultColors[previewAppearance][key],
            savedColors[previewAppearance][key],
            ...presetColorsFor(previewAppearance, key),
        ].filter((value) => {
            const normalized = value.toLowerCase();
            if (seen.has(normalized)) return false;
            seen.add(normalized);
            return true;
        });
    };

    return (
        <ThemeEditorSection
            title="Colors"
            control={control}
            paths={SECTION_FIELDS.colors}
            defaultOpen
            onReset={onReset}
        >
            {/* One scheme at a time, and the switch moves the preview with it: the colours being edited
                and the frame they are judged in are the same choice, so they are the same control. */}
            <div
                role="radiogroup"
                aria-label="Scheme being edited"
                className="flex overflow-hidden rounded-lg border bg-background"
            >
                {SCHEMES.map((scheme) => (
                    <button
                        key={scheme}
                        type="button"
                        role="radio"
                        aria-checked={previewAppearance === scheme}
                        onClick={() => onPreviewAppearanceChange(scheme)}
                        className={cn(
                            "h-9 flex-1 border-l text-sm transition-colors first:border-l-0 focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none focus-visible:ring-inset",
                            previewAppearance === scheme
                                ? "bg-muted font-medium text-foreground"
                                : "text-muted-foreground hover:bg-accent",
                        )}
                    >
                        {scheme}
                    </button>
                ))}
            </div>
            <div className="grid gap-1.5">
                {COLORS.map((color) => (
                    <ThemeEditorColorRow
                        key={color.label}
                        control={control}
                        // The two schemes hold the same three colours, so the field name is the scheme's
                        // prefix and the colour's own name.
                        name={`${previewAppearance.toLowerCase()}${capitalize(color.key)}` as keyof WidgetThemeFormData}
                        label={color.label}
                        disabled={isSaving}
                        presets={presetsFor(color.key)}
                    />
                ))}
            </div>
        </ThemeEditorSection>
    );
}
