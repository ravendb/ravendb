import { zodResolver } from "@hookform/resolvers/zod";
import { Braces, Layers, Moon, Sun } from "lucide-react";
import { useState } from "react";
import { useForm, useWatch } from "react-hook-form";
import { z } from "zod";
import AceEditor from "@/components/ace-editor/ace-editor";
import { formatCss } from "@/components/ace-editor/ace-editor-action-utils";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { FormRadioCards, type RadioCardOption } from "@/components/form/form-radio-cards";
import { useUnsavedChanges } from "@/components/form/unsaved-changes/use-unsaved-changes";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { WebWidgetStylePreview } from "@/pages/apps/channels/web-widget-style-preview";
import {
    buildThemeCss,
    changedThemeVariables,
    parseThemeVariables,
    type ThemeVariableName,
    type ThemeVariableValues,
} from "@/pages/apps/channels/web-widget-theme-css";
import { WebWidgetThemeVariables } from "@/pages/apps/channels/web-widget-theme-variables";

// Mirrors the server-side IFrameCss.MaxLength cap so the editor fails fast.
const MAX_CSS_LENGTH = 100_000;

// "Default" is the widget editor's "follow the app default" choice; the rest mirror IFrameStyle.
const styleSchema = z
    .object({
        style: z.enum(["Default", "Light", "Dark", "Custom"]),
        css: z.string().max(MAX_CSS_LENGTH, `CSS must be ${MAX_CSS_LENGTH.toLocaleString()} characters or fewer`),
    })
    .refine((values) => values.style !== "Custom" || values.css.trim().length > 0, {
        message: "Custom CSS must not be empty",
        path: ["css"],
    });

type StyleFormData = z.infer<typeof styleSchema>;

type PresetStyle = Exclude<StyleFormData["style"], "Custom">;

export type WebWidgetStyle = "Light" | "Dark" | "Custom";

export type WebWidgetStyleUpdate = { style: WebWidgetStyle | null; css: string | null };

type WebWidgetStyleEditorProps = {
    /** The saved style choice. Null means "follow the app default" (widget editor only). */
    initialStyle: WebWidgetStyle | null;
    initialCss: string;
    /** The resolved app-wide default, offered as an extra "App default" choice.
     *  Present only in the per-widget editor. */
    appDefault?: { style: WebWidgetStyle; css: string };
    /** The widget's full base stylesheet, used to pre-fill the custom-CSS editor when there is
     *  nothing saved yet, so operators see real selectors instead of a blank page. */
    baseCss: string;
    /** The built-in presets' :root variable blocks, injected into the preview to render them live. */
    lightThemeCss: string;
    darkThemeCss: string;
    previewHtml: string;
    isSaving: boolean;
    onSave: (update: WebWidgetStyleUpdate) => void;
};

export function WebWidgetStyleEditor({
    initialStyle,
    initialCss,
    appDefault,
    baseCss,
    lightThemeCss,
    darkThemeCss,
    previewHtml,
    isSaving,
    onSave,
}: WebWidgetStyleEditorProps) {
    const form = useForm<StyleFormData>({
        resolver: zodResolver(styleSchema),
        values: {
            style: initialStyle ?? (appDefault ? "Default" : "Light"),
            css: formatCss(initialCss || baseCss),
        },
    });

    const [style, css] = useWatch({ control: form.control, name: ["style", "css"] });
    const isCustom = style === "Custom";

    // Pending per-preset variable edits, kept per style so comparing presets doesn't lose tweaks.
    const [overridesByStyle, setOverridesByStyle] = useState<Partial<Record<PresetStyle, ThemeVariableValues>>>({});

    // A save re-syncs the initial values; the pending tweaks are then saved (as custom CSS) or
    // obsolete, so drop them lest re-selecting a preset resurrects them.
    const [prevInitial, setPrevInitial] = useState({ initialStyle, initialCss });
    if (prevInitial.initialStyle !== initialStyle || prevInitial.initialCss !== initialCss) {
        setPrevInitial({ initialStyle, initialCss });
        setOverridesByStyle({});
    }

    const presetCss = (preset: WebWidgetStyle, customCss: string) =>
        preset === "Custom" ? customCss : preset === "Dark" ? darkThemeCss : lightThemeCss;
    const baseThemeCss =
        style === "Default"
            ? appDefault
                ? presetCss(appDefault.style, appDefault.css)
                : lightThemeCss
            : presetCss(style, css);

    // The preview's base stylesheet declares the Light variables, so a custom app default
    // resolves to Light values for anything its CSS doesn't override.
    const themeDefaults: ThemeVariableValues = isCustom
        ? {}
        : { ...parseThemeVariables(lightThemeCss), ...parseThemeVariables(baseThemeCss) };
    const themeOverrides = (!isCustom && overridesByStyle[style]) || {};
    const themeChanges = changedThemeVariables(themeDefaults, themeOverrides);
    const hasThemeChanges = Object.keys(themeChanges).length > 0;

    const previewCss = hasThemeChanges ? `${baseThemeCss}\n${buildThemeCss(themeChanges)}` : baseThemeCss;

    // A save re-syncs the initial values and drops the pending tweaks, so both flags clear on their own.
    useUnsavedChanges((form.formState.isDirty || hasThemeChanges) && !isSaving);

    const setThemeVariable = (name: ThemeVariableName, value: string) => {
        if (isCustom) return;
        setOverridesByStyle((current) => ({ ...current, [style]: { ...current[style], [name]: value } }));
    };

    const styleOptions: RadioCardOption<StyleFormData["style"]>[] = [
        ...(appDefault
            ? [
                  {
                      value: "Default",
                      label: "App default",
                      description:
                          appDefault.style === "Custom"
                              ? "Follow the app-wide default — currently custom CSS."
                              : `Follow the app-wide default — currently the ${appDefault.style} style.`,
                      icon: <Layers className="size-5" />,
                  } satisfies RadioCardOption<StyleFormData["style"]>,
              ]
            : []),
        {
            value: "Light",
            label: "Light",
            description: "Clean white background with blue accents.",
            icon: <Sun className="size-5" />,
        },
        {
            value: "Dark",
            label: "Dark",
            description: "Deep navy background with blue accents.",
            icon: <Moon className="size-5" />,
        },
        {
            value: "Custom",
            label: "Custom CSS",
            description: "Write your own CSS on top of the widget's base styles.",
            icon: <Braces className="size-5" />,
        },
    ];

    return (
        <form
            className="grid gap-4"
            onSubmit={form.handleSubmit((values) => {
                if (values.style !== "Custom" && hasThemeChanges) {
                    // A tweaked preset becomes custom CSS: the full merged variable set, prefixed with a
                    // custom app default's CSS so its non-variable rules survive the conversion.
                    const rootBlock = buildThemeCss({ ...themeDefaults, ...themeChanges });
                    const customDefaultCss =
                        values.style === "Default" && appDefault?.style === "Custom"
                            ? `${appDefault.css.trim()}\n\n`
                            : "";
                    onSave({ style: "Custom", css: `${customDefaultCss}${rootBlock}` });
                    return;
                }
                onSave({
                    style: values.style === "Default" ? null : values.style,
                    css: values.style === "Custom" ? values.css.trim() : null,
                });
            })}
        >
            <div className="flex items-center justify-end gap-2">
                {isCustom && (
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={isSaving}
                        onClick={() =>
                            form.setValue("css", formatCss(baseCss), { shouldDirty: true, shouldValidate: true })
                        }
                    >
                        Reset to base styles
                    </Button>
                )}
                {!isCustom && hasThemeChanges && (
                    <Button
                        type="button"
                        variant="outline"
                        size="sm"
                        disabled={isSaving}
                        onClick={() => setOverridesByStyle((current) => ({ ...current, [style]: {} }))}
                    >
                        Reset changes
                    </Button>
                )}
                <Button type="submit" size="sm" disabled={(!form.formState.isDirty && !hasThemeChanges) || isSaving}>
                    {isSaving && <Spinner />}
                    Save
                </Button>
            </div>

            <FormRadioCards
                control={form.control}
                name="style"
                options={styleOptions}
                disabled={isSaving}
                className={appDefault ? "sm:grid-cols-2 xl:grid-cols-4" : "sm:grid-cols-3"}
            />

            <div className="grid gap-4 lg:grid-cols-2">
                {isCustom ? (
                    <FormAceEditor
                        control={form.control}
                        name="css"
                        mode="css"
                        label="Custom CSS"
                        height="560px"
                        actions={[
                            { component: <AceEditor.FormatAction /> },
                            { component: <AceEditor.FullScreenAction /> },
                        ]}
                        labelClassName="w-full justify-center"
                    />
                ) : (
                    <div className="grid content-start gap-1.5">
                        <span className="text-center text-sm font-medium">Theme variables</span>
                        <WebWidgetThemeVariables
                            values={{ ...themeDefaults, ...themeOverrides }}
                            disabled={isSaving}
                            onValueChange={setThemeVariable}
                        />
                    </div>
                )}

                <div className="grid content-start gap-1.5">
                    <span className="text-center text-sm font-medium">Live preview</span>
                    <div className="mx-auto h-[560px] w-full max-w-[420px] overflow-hidden rounded-lg border">
                        <WebWidgetStylePreview previewHtml={previewHtml} css={previewCss} />
                    </div>
                </div>
            </div>
        </form>
    );
}
