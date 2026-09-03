import { zodResolver } from "@hookform/resolvers/zod";
import { ChevronDown } from "lucide-react";
import { useState, type ReactNode } from "react";
import { useForm, useWatch, type Control, type FieldPath } from "react-hook-form";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { FormColorPicker } from "@/components/form/form-color-picker";
import { FormErrorIcon } from "@/components/form/form-error-icon";
import { FormImagePicker } from "@/components/form/form-image-picker";
import { FormInput } from "@/components/form/form-input";
import { FormSelect } from "@/components/form/form-select";
import { FormStringList } from "@/components/form/form-string-list";
import { FormSwitch } from "@/components/form/form-switch";
import { FormToggleGroup } from "@/components/form/form-toggle-group";
import { useFormUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Heading, Text } from "@/components/typography";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from "@/components/shadcn/ui/collapsible";
import { Separator } from "@/components/shadcn/ui/separator";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/shadcn/ui/tabs";
import { ToggleGroup, ToggleGroupItem } from "@/components/shadcn/ui/toggle-group";
import {
    FONT_SIZE_OPTIONS,
    LOGO_RADIUS_OPTIONS,
    MAX_CUSTOM_FONT_SIZE_REM,
    MAX_SUGGESTED_PROMPTS,
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
import { InlineCode } from "@/components/data/inline-code";

const APPEARANCE_OPTIONS = [
    { value: "Light", label: "Light" },
    { value: "Dark", label: "Dark" },
    { value: "System", label: "System" },
] as const;

const SCHEME_COLOR_FIELDS = {
    Light: ["lightButtonColor", "lightMessageColor", "lightBackgroundColor"],
    Dark: ["darkButtonColor", "darkMessageColor", "darkBackgroundColor"],
} as const satisfies Record<PreviewAppearance, readonly (keyof WidgetThemeFormData)[]>;

const SECTION_FIELDS = {
    colors: [...SCHEME_COLOR_FIELDS.Light, ...SCHEME_COLOR_FIELDS.Dark],
    style: ["radius", "fontFamily", "fontSize", "customFontSizeRem"],
    branding: ["showHeader", "logo", "logoRadius", "headerTitle", "headerSubtitle"],
    content: ["greetingTitle", "greetingBody", "suggestedPrompts", "inputPlaceholder", "disclaimer"],
    customCss: ["customCss"],
} as const satisfies Record<string, readonly FieldPath<WidgetThemeFormData>[]>;

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
    control,
    paths,
    defaultOpen = false,
    children,
}: {
    title: string;
    control: Control<WidgetThemeFormData>;
    paths: readonly FieldPath<WidgetThemeFormData>[];
    defaultOpen?: boolean;
    children: ReactNode;
}) {
    const [isOpen, setIsOpen] = useState(defaultOpen);

    return (
        <Collapsible open={isOpen} onOpenChange={setIsOpen} className="rounded-md border bg-card p-4" asChild>
            <section>
                <Heading as="h3" variant="subsection">
                    <CollapsibleTrigger className="group flex w-full items-center justify-between gap-3 rounded-sm text-left focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none">
                        <span className="flex items-center gap-1.5">
                            {title}
                            <FormErrorIcon control={control} paths={paths} onError={() => setIsOpen(true)} />
                        </span>
                        <ChevronDown
                            className="size-4 shrink-0 text-muted-foreground transition-transform group-data-[state=open]:rotate-180"
                            aria-hidden="true"
                        />
                    </CollapsibleTrigger>
                </Heading>
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

    // The save is mutation-driven, so the window to suppress is the request itself - the saved theme comes
    // back through `values`, which resets the form. A failed save leaves the edits dirty, re-arming the guard.
    useFormUnsavedChanges(form, { isSaving });

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
        <form
            className="grid gap-6"
            onSubmit={form.handleSubmit(
                (submitted) => onSave(toWidgetTheme(submitted)),
                (errors) => {
                    // The color fields of the inactive scheme are unmounted with their tab, so an error there
                    // would be invisible and Save would appear to do nothing - switch to the tab that has it.
                    const hasColorError = (scheme: PreviewAppearance) =>
                        SCHEME_COLOR_FIELDS[scheme].some((field) => field in errors);
                    const otherScheme = previewAppearance === "Light" ? "Dark" : "Light";
                    if (!hasColorError(previewAppearance) && hasColorError(otherScheme)) {
                        setPreviewAppearance(otherScheme);
                    }
                },
            )}
        >
            <div className="sticky top-0 z-10 -mx-2 flex flex-wrap items-center justify-end gap-2 bg-surface1 px-2 py-2 dark:bg-surface2">
                {canFollowAppDefault && !isFollowingAppDefault && (
                    <Button type="button" variant="outline" size="sm" disabled={isSaving} onClick={() => onSave(null)}>
                        Follow app default
                    </Button>
                )}
                <Button type="submit" size="sm" disabled={isSaving}>
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
                                <span>
                                    System follows the visitor's own preference. The embedding page can override this
                                    with{" "}
                                    <InlineCode className="whitespace-nowrap">?appearance=dark|light|system</InlineCode>{" "}
                                    on the embed URL or an appearance message - see “Embed on your own site” on the
                                    channel page.
                                </span>
                            }
                            options={APPEARANCE_OPTIONS}
                            canDeselect={false}
                            onValueChange={(next) => {
                                if (next === "Light" || next === "Dark") setPreviewAppearance(next);
                            }}
                            disabled={isSaving}
                        />
                    </div>

                    <Section title="Colors" control={form.control} paths={SECTION_FIELDS.colors} defaultOpen>
                        <Text variant="muted">
                            Each scheme keeps its own colors. Every other option applies to both.
                        </Text>
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

                    <Section title="Style" control={form.control} paths={SECTION_FIELDS.style}>
                        <FormSelect
                            control={form.control}
                            name="radius"
                            label="Radius"
                            description="Rounds the corners inside the widget - message bubbles, code blocks, the composer and the prompt pills."
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

                    <Section title="Branding" control={form.control} paths={SECTION_FIELDS.branding}>
                        <FormSwitch
                            control={form.control}
                            name="showHeader"
                            label="Show the header"
                            disabled={isSaving}
                        />
                        {previewTheme.showHeader === false && (
                            <Text variant="muted">
                                The header is hidden, so nothing below is shown to visitors. It is kept for when you
                                turn the header back on.
                            </Text>
                        )}
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

                    <Section title="Content" control={form.control} paths={SECTION_FIELDS.content}>
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

                    <Section title="Custom CSS" control={form.control} paths={SECTION_FIELDS.customCss}>
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

                <div className="grid gap-3 xl:sticky xl:top-14">
                    <div className="flex flex-wrap items-center justify-between gap-2">
                        <Text as="span" variant="label">
                            Live preview
                        </Text>
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
                            <Separator orientation="vertical" />
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
