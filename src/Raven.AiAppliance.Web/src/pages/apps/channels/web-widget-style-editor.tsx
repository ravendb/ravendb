import { zodResolver } from "@hookform/resolvers/zod";
import { useForm, useWatch } from "react-hook-form";
import { z } from "zod";
import AceEditor from "@/components/ace-editor/ace-editor";
import { formatCss } from "@/components/ace-editor/ace-editor-action-utils";
import { FormAceEditor } from "@/components/form/form-ace-editor";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { WebWidgetStylePreview } from "@/pages/apps/channels/web-widget-style-preview";

// Mirrors the server-side IFrameCss.MaxLength cap so the editor fails fast.
const MAX_CSS_LENGTH = 100_000;

const styleSchema = z.object({
    css: z.string().max(MAX_CSS_LENGTH, `CSS must be ${MAX_CSS_LENGTH.toLocaleString()} characters or fewer`),
});

type StyleFormData = z.infer<typeof styleSchema>;

type WebWidgetStyleEditorProps = {
    initialCss: string;
    /** App default styles for a channel, used as the effective CSS shown in the preview and
     *  restored by "Reset to default". Omit for the default editor, which falls back to the base styles. */
    defaultCss?: string;
    /** The widget's full base stylesheet, used to pre-fill the editor when there is
     *  nothing saved yet, so operators see real selectors instead of a blank page. */
    baseCss: string;
    previewHtml: string;
    isSaving: boolean;
    onSave: (css: string) => void;
};

export function WebWidgetStyleEditor({
    initialCss,
    defaultCss,
    baseCss,
    previewHtml,
    isSaving,
    onSave,
}: WebWidgetStyleEditorProps) {
    const effectiveCss = formatCss(defaultCss || baseCss);
    const startingCss = initialCss || effectiveCss;

    const form = useForm<StyleFormData>({
        resolver: zodResolver(styleSchema),
        values: { css: startingCss },
    });

    const css = useWatch({ control: form.control, name: "css" });
    const effectiveCssTrimmed = effectiveCss.trim();
    const previewCss = css.trim() ? css : effectiveCss;

    return (
        <form
            className="grid gap-4"
            onSubmit={form.handleSubmit((values) => {
                const trimmed = values.css.trim();
                // Save empty when the editor still matches the effective default so the record clears
                // to "inherit the default" instead of freezing a copy that stops tracking future changes.
                onSave(trimmed === effectiveCssTrimmed ? "" : trimmed);
            })}
        >
            <div className="flex items-center justify-end gap-2">
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    disabled={isSaving}
                    onClick={() => form.setValue("css", effectiveCss, { shouldDirty: true, shouldValidate: true })}
                >
                    Reset to default
                </Button>
                <Button type="submit" size="sm" disabled={!form.formState.isDirty || isSaving}>
                    {isSaving && <Spinner />}
                    Save
                </Button>
            </div>

            <div className="grid gap-4 lg:grid-cols-2">
                <FormAceEditor
                    control={form.control}
                    name="css"
                    mode="css"
                    label="Custom CSS"
                    height="560px"
                    actions={[{ component: <AceEditor.FormatAction /> }, { component: <AceEditor.FullScreenAction /> }]}
                    labelClassName="w-full justify-center"
                />

                <div className="grid content-start gap-1.5">
                    <span className="text-center text-sm font-medium">Live preview</span>
                    <div className="mx-auto h-[560px] w-full max-w-[420px] overflow-hidden rounded-lg border bg-white">
                        <WebWidgetStylePreview previewHtml={previewHtml} css={previewCss} />
                    </div>
                </div>
            </div>
        </form>
    );
}
