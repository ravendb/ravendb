import { zodResolver } from "@hookform/resolvers/zod";
import { Monitor, Moon, Smartphone, Sun } from "lucide-react";
import { useState, type ReactNode } from "react";
import { useForm, useWatch } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { FormColorPicker } from "@/components/form/form-color-picker";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormSlider } from "@/components/form/form-slider";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import { FormToggleGroup } from "@/components/form/form-toggle-group";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Separator } from "@/components/shadcn/ui/separator";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import {
    ACCENT_PRESETS,
    MAX_PROMPTS,
    RADIUS_MAX,
    toFormData,
    toPreviewTheme,
    toWidgetTheme,
    widgetThemeSchema,
    type WidgetThemeFormData,
    type WidgetThemeFormOutput,
} from "@/pages/apps/channels/web-widget-theme-schema";
import {
    WebWidgetThemePreview,
    type PreviewAppearance,
    type PreviewDevice,
} from "@/pages/apps/channels/web-widget-theme-preview";

const APPEARANCE_OPTIONS = [
    { value: "Light", label: "Light" },
    { value: "Dark", label: "Dark" },
    { value: "System", label: "System" },
] as const;

const DENSITY_OPTIONS = [
    { value: "Comfortable", label: "Comfortable" },
    { value: "Compact", label: "Compact" },
] as const;

type WebWidgetThemeEditorProps = {
    /** The saved theme. Null means "follow the app default", which only the per-widget editor offers. */
    theme: WidgetTheme | null;
    /** The resolved app default, edited directly by the app-level editor and followed by the per-widget one. */
    defaultTheme: WidgetTheme;
    /** Curated font stacks, served with the theme so the dashboard never keeps its own copy. */
    fontOptions: WidgetFontOption[];
    /** Present only in the per-widget editor, which can hand control back to the app default. */
    canFollowAppDefault?: boolean;
    isSaving: boolean;
    onSave: (theme: WidgetTheme | null) => void;
};

function Section({ title, children }: { title: string; children: ReactNode }) {
    return (
        <section className="grid gap-4">
            <h3 className="text-sm font-semibold">{title}</h3>
            {children}
        </section>
    );
}

export function WebWidgetThemeEditor({
    theme,
    defaultTheme,
    fontOptions,
    canFollowAppDefault = false,
    isSaving,
    onSave,
}: WebWidgetThemeEditorProps) {
    const isFollowingAppDefault = canFollowAppDefault && theme === null;

    const savedTheme = theme ?? defaultTheme;

    const form = useForm<WidgetThemeFormData, unknown, WidgetThemeFormOutput>({
        resolver: zodResolver(widgetThemeSchema),
        // The app default seeds the form while a widget follows it, so switching to an own theme starts from
        // what the operator is already looking at instead of from the built-in.
        values: toFormData(savedTheme),
    });

    const [appearancePreview, setAppearancePreview] = useState<PreviewAppearance | null>(null);
    const [device, setDevice] = useState<PreviewDevice>("desktop");

    const previewTheme = toPreviewTheme(useWatch({ control: form.control }), savedTheme);

    const appearance: PreviewAppearance = appearancePreview ?? (previewTheme.appearance === "Dark" ? "Dark" : "Light");

    const fontSelectOptions = fontOptions.map((option) => ({ value: option.stack, label: option.label }));
    // A hand-written stack saved earlier is not in the curated list; offer it so the select can show it.
    const hasCuratedFont = fontSelectOptions.some((option) => option.value === previewTheme.fontFamily);

    return (
        <form className="grid gap-6" onSubmit={form.handleSubmit((submitted) => onSave(toWidgetTheme(submitted)))}>
            <div className="flex flex-wrap items-center justify-end gap-2">
                {canFollowAppDefault && !isFollowingAppDefault && (
                    <Button type="button" variant="outline" size="sm" disabled={isSaving} onClick={() => onSave(null)}>
                        Follow app default
                    </Button>
                )}
                <Button
                    type="submit"
                    size="sm"
                    disabled={isSaving || (!form.formState.isDirty && !isFollowingAppDefault)}
                >
                    {isSaving && <Spinner />}
                    Save
                </Button>
            </div>

            {isFollowingAppDefault && (
                <Alert>
                    This widget follows the app-wide default. Change anything below and save to give it a theme of its
                    own.
                </Alert>
            )}

            <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,1fr)_minmax(0,1fr)]">
                <div className="grid gap-6">
                    <Section title="Appearance">
                        <FormToggleGroup
                            control={form.control}
                            name="appearance"
                            label="Colour scheme"
                            description="System follows each visitor's own light or dark preference."
                            options={APPEARANCE_OPTIONS}
                            canDeselect={false}
                            disabled={isSaving}
                        />
                        <FormColorPicker
                            control={form.control}
                            name="accentColor"
                            label="Accent colour"
                            description="Every other colour is derived from this, so light and dark both stay coherent."
                            presets={ACCENT_PRESETS}
                            disabled={isSaving}
                        />
                        <FormSlider
                            control={form.control}
                            name="radius"
                            label="Corner radius"
                            min={0}
                            max={RADIUS_MAX}
                            formatValue={(value) => `${value}px`}
                            disabled={isSaving}
                        />
                        <FormSelect
                            control={form.control}
                            name="fontFamily"
                            label="Font"
                            options={
                                hasCuratedFont
                                    ? fontSelectOptions
                                    : [...fontSelectOptions, { value: previewTheme.fontFamily, label: "Custom" }]
                            }
                            disabled={isSaving}
                        />
                        <FormToggleGroup
                            control={form.control}
                            name="density"
                            label="Density"
                            options={DENSITY_OPTIONS}
                            canDeselect={false}
                            disabled={isSaving}
                        />
                    </Section>

                    <Separator />

                    <Section title="Branding">
                        <FormSwitch
                            control={form.control}
                            name="showHeader"
                            label="Show the header"
                            disabled={isSaving}
                        />
                        <FormInput
                            control={form.control}
                            name="headerTitle"
                            label="Title"
                            placeholder="AI Assistant"
                            disabled={isSaving}
                        />
                        <FormInput
                            control={form.control}
                            name="headerSubtitle"
                            label="Subtitle"
                            placeholder="Ask me anything"
                            disabled={isSaving}
                        />
                        <FormInput
                            control={form.control}
                            name="avatarInitials"
                            label="Avatar initials"
                            placeholder="AI"
                            maxLength={3}
                            description="Up to three characters. Left blank, the title's initials are used."
                            disabled={isSaving}
                        />
                    </Section>

                    <Separator />

                    <Section title="Content">
                        <FormInput
                            control={form.control}
                            name="greetingTitle"
                            label="Greeting title"
                            placeholder="How can I help?"
                            disabled={isSaving}
                        />
                        <FormInput
                            control={form.control}
                            name="greetingBody"
                            label="Greeting body"
                            placeholder="Ask a question and I'll do my best to answer it."
                            disabled={isSaving}
                        />
                        <FormStringList
                            control={form.control}
                            name="suggestedPrompts"
                            label="Suggested prompts"
                            description={`Shown as pills on the empty state. Up to ${MAX_PROMPTS}.`}
                            addButtonLabel="Add prompt"
                            emptyLabel="No suggested prompts."
                            defaultValue={{ value: "" }}
                            fieldName={(index) => `suggestedPrompts.${index}.value`}
                            placeholder="Where is my order?"
                        />
                        <FormInput
                            control={form.control}
                            name="inputPlaceholder"
                            label="Input placeholder"
                            placeholder="Ask a question..."
                            disabled={isSaving}
                        />
                        <FormInput
                            control={form.control}
                            name="disclaimer"
                            label="Disclaimer"
                            placeholder="AI responses may be inaccurate."
                            description="Shown as a small line under the composer. Left blank, nothing is shown."
                            disabled={isSaving}
                        />
                    </Section>
                </div>

                <div className="grid gap-3 xl:sticky xl:top-4">
                    <div className="flex items-center justify-between gap-2">
                        <span className="text-sm font-medium">Live preview</span>
                        <div className="flex gap-2">
                            <ToggleGroup
                                type="single"
                                value={appearance}
                                onValueChange={(next) => next && setAppearancePreview(next as PreviewAppearance)}
                            >
                                <ToggleGroupItem value="Light" aria-label="Preview in light mode">
                                    <Sun className="size-4" />
                                </ToggleGroupItem>
                                <ToggleGroupItem value="Dark" aria-label="Preview in dark mode">
                                    <Moon className="size-4" />
                                </ToggleGroupItem>
                            </ToggleGroup>
                            <ToggleGroup
                                type="single"
                                value={device}
                                onValueChange={(next) => next && setDevice(next as PreviewDevice)}
                            >
                                <ToggleGroupItem value="desktop" aria-label="Preview at desktop width">
                                    <Monitor className="size-4" />
                                </ToggleGroupItem>
                                <ToggleGroupItem value="mobile" aria-label="Preview at mobile width">
                                    <Smartphone className="size-4" />
                                </ToggleGroupItem>
                            </ToggleGroup>
                        </div>
                    </div>

                    <WebWidgetThemePreview theme={previewTheme} appearance={appearance} device={device} />
                </div>
            </div>
        </form>
    );
}
