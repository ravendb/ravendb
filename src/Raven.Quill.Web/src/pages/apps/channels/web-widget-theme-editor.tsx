import { zodResolver } from "@hookform/resolvers/zod";
import { ChevronDown } from "lucide-react";
import { useState, type ReactNode } from "react";
import { useForm, useWatch } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { FormColorPicker } from "@/components/form/form-color-picker";
import { FormImagePicker } from "@/components/form/form-image-picker";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import { FormToggleGroup } from "@/components/form/form-toggle-group";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import {
    FONT_SIZE_OPTIONS,
    LOGO_RADIUS_OPTIONS,
    MAX_CUSTOM_FONT_SIZE_REM,
    MAX_PROMPTS,
    MIN_CUSTOM_FONT_SIZE_REM,
    RADIUS_OPTIONS,
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
    type PreviewView,
} from "@/pages/apps/channels/web-widget-theme-preview";

const APPEARANCE_OPTIONS = [
    { value: "Light", label: "Light" },
    { value: "Dark", label: "Dark" },
    { value: "System", label: "System" },
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

function Section({
    title,
    defaultOpen = false,
    children,
}: {
    title: string;
    defaultOpen?: boolean;
    children: ReactNode;
}) {
    return (
        <Collapsible defaultOpen={defaultOpen} className="rounded-md border bg-card p-4" asChild>
            <section>
                <h3 className="text-sm font-semibold">
                    <CollapsibleTrigger className="group flex w-full items-center justify-between gap-3 rounded-sm text-left focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none">
                        {title}
                        <ChevronDown
                            className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                            aria-hidden="true"
                        />
                    </CollapsibleTrigger>
                </h3>
                <CollapsibleContent className="mt-4 grid gap-4">{children}</CollapsibleContent>
            </section>
        </Collapsible>
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

    // One state drives both the colors tabs and the previewed scheme, so the colors on screen are always
    // the colors in the frame and editing dark never means guessing.
    const [previewAppearance, setPreviewAppearance] = useState<PreviewAppearance>(
        savedTheme.appearance === "Dark" ? "Dark" : "Light",
    );
    const [previewView, setPreviewView] = useState<PreviewView>("Conversation");

    const previewTheme = toPreviewTheme(useWatch({ control: form.control }), savedTheme);

    const fontSelectOptions = fontOptions.map((option) => ({ value: option.stack, label: option.label }));
    // A hand-written stack saved earlier is not in the curated list; offer it so the select can show it.
    const hasCuratedFont = fontSelectOptions.some((option) => option.value === previewTheme.fontFamily);

    const colorFields = (scheme: PreviewAppearance) => (
        <>
            <FormColorPicker
                control={form.control}
                name={scheme === "Light" ? "lightButtonColor" : "darkButtonColor"}
                label="Button color"
                description="Buttons, links and highlights."
                disabled={isSaving}
            />
            <FormColorPicker
                control={form.control}
                name={scheme === "Light" ? "lightMessageColor" : "darkMessageColor"}
                label="Message color"
                description="The visitor's message bubbles."
                disabled={isSaving}
            />
            <FormColorPicker
                control={form.control}
                name={scheme === "Light" ? "lightBackgroundColor" : "darkBackgroundColor"}
                label="Background color"
                disabled={isSaving}
            />
        </>
    );

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

            <div className="grid items-start gap-6 xl:grid-cols-[minmax(0,5fr)_minmax(0,6fr)]">
                <div className="grid gap-4">
                    <div className="rounded-md border bg-card p-4">
                        <FormToggleGroup
                            control={form.control}
                            name="appearance"
                            label="Default color scheme"
                            description={
                                "System follows each visitor's own light or dark preference. The embedding page can " +
                                "override this per visitor: append ?appearance=dark (or light/system) to the embed " +
                                "URL, or post an appearance message when its own theme toggles - see " +
                                "“Embed on your own site” on the channel page."
                            }
                            options={APPEARANCE_OPTIONS}
                            canDeselect={false}
                            onValueChange={(next) => {
                                if (next === "Light" || next === "Dark") setPreviewAppearance(next);
                            }}
                            disabled={isSaving}
                        />
                    </div>

                    <Section title="Colors" defaultOpen>
                        <p className="text-sm text-muted-foreground">
                            Each scheme keeps its own colors; every other option applies to both.
                        </p>
                        <Tabs
                            value={previewAppearance}
                            onValueChange={(next) => setPreviewAppearance(next as PreviewAppearance)}
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
                    </Section>

                    <Section title="Style">
                        <FormSelect
                            control={form.control}
                            name="radius"
                            label="Radius"
                            description="Rounds every corner the widget draws, from surfaces to the prompt pills."
                            options={RADIUS_OPTIONS}
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
                        <FormSelect
                            control={form.control}
                            name="fontSize"
                            label="Font size"
                            options={FONT_SIZE_OPTIONS}
                            disabled={isSaving}
                        />
                        {previewTheme.fontSize === "Custom" && (
                            <FormInput
                                control={form.control}
                                name="customFontSizeRem"
                                label="Custom font size (rem)"
                                type="number"
                                step="0.025"
                                min={MIN_CUSTOM_FONT_SIZE_REM}
                                max={MAX_CUSTOM_FONT_SIZE_REM}
                                placeholder="1"
                                description={`1 is the standard size; ${MIN_CUSTOM_FONT_SIZE_REM}-${MAX_CUSTOM_FONT_SIZE_REM}.`}
                                disabled={isSaving}
                            />
                        )}
                    </Section>

                    <Section title="Branding">
                        <FormImagePicker
                            control={form.control}
                            name="logo"
                            label="Logo"
                            description="Shown in the header next to the title. Downscaled to 128px and stored with the theme."
                            disabled={isSaving}
                        />
                        <FormSelect
                            control={form.control}
                            name="logoRadius"
                            label="Logo radius"
                            description="Rounds only the logo. Pill crops a square logo to a circle."
                            options={LOGO_RADIUS_OPTIONS}
                            disabled={isSaving}
                        />
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
                    </Section>

                    <Section title="Content">
                        {/* These render only on the welcome screen, so editing them steers the preview there. */}
                        <div className="grid gap-4" onFocusCapture={() => setPreviewView("Welcome")}>
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
                                description={`Offered on the welcome screen. Up to ${MAX_PROMPTS}.`}
                                addButtonLabel="Add prompt"
                                emptyLabel="No suggested prompts."
                                defaultValue={{ value: "" }}
                                fieldName={(index) => `suggestedPrompts.${index}.value`}
                                placeholder="Where is my order?"
                            />
                        </div>
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

                    <Section title="Custom CSS">
                        <FormAceEditor
                            control={form.control}
                            name="customCss"
                            mode="css"
                            height="220px"
                            disabled={isSaving}
                            description="Appended after the widget's own styles, for anything the options above don't cover — scrollbars, spacing, one-off tweaks."
                        />
                    </Section>
                </div>

                <div className="grid gap-3 xl:sticky xl:top-4">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                        <span className="text-sm font-medium">Live preview</span>
                        <div className="flex flex-wrap items-center gap-3">
                            <ToggleGroup
                                type="single"
                                variant="outline"
                                size="sm"
                                value={previewView}
                                onValueChange={(next) => {
                                    if (next !== "") setPreviewView(next as PreviewView);
                                }}
                                aria-label="Previewed screen"
                            >
                                <ToggleGroupItem value="Welcome">Welcome</ToggleGroupItem>
                                <ToggleGroupItem value="Conversation">Conversation</ToggleGroupItem>
                            </ToggleGroup>
                            <ToggleGroup
                                type="single"
                                variant="outline"
                                size="sm"
                                value={previewAppearance}
                                onValueChange={(next) => {
                                    if (next !== "") setPreviewAppearance(next as PreviewAppearance);
                                }}
                                aria-label="Previewed color scheme"
                            >
                                <ToggleGroupItem value="Light">Light</ToggleGroupItem>
                                <ToggleGroupItem value="Dark">Dark</ToggleGroupItem>
                            </ToggleGroup>
                        </div>
                    </div>
                    <WebWidgetThemePreview theme={previewTheme} appearance={previewAppearance} view={previewView} />
                </div>
            </div>
        </form>
    );
}
