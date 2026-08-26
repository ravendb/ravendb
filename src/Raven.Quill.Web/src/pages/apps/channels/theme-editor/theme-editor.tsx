import { useState } from "react";
import type { WidgetFontOption, WidgetTheme } from "@/api/generated/server-api";
import { Alert } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { Spinner } from "@/components/shadcn/ui/spinner";
import { SCHEME_COLOR_FIELDS } from "@/pages/apps/channels/theme-editor/theme-editor-fields";
import { ThemeEditorInspector } from "@/pages/apps/channels/theme-editor/theme-editor-inspector";
import { ThemeEditorStage } from "@/pages/apps/channels/theme-editor/theme-editor-stage";
import { useThemeEditorForm } from "@/pages/apps/channels/theme-editor/use-theme-editor-form";
import { type PreviewAppearance, type PreviewView } from "@/pages/apps/channels/web-widget-theme-preview";
import { toWidgetTheme } from "@/pages/apps/channels/web-widget-theme-schema";

type WebWidgetThemeEditorProps = {
    /** The saved theme. Null means "follow the app default", which only the per-widget editor offers. */
    theme: WidgetTheme | null;
    /** The resolved app default, edited directly by the app-level editor and followed by the per-widget one. */
    defaultTheme: WidgetTheme;
    /** Curated font stacks, served with the theme so the dashboard never keeps its own copy. */
    fontOptions: WidgetFontOption[];
    /** Present only in the per-widget editor, which can hand control back to the app default. */
    canFollowAppDefault?: boolean;
    /** Present only in the app-level editor, the one theme with no other theme to fall back to. */
    canResetToBuiltIn?: boolean;
    isSaving: boolean;
    /** Resolves once the theme has reached the server; rejects so a failed save leaves the form dirty. */
    onSave: (theme: WidgetTheme | null) => Promise<unknown>;
};

export function WebWidgetThemeEditor({
    theme,
    defaultTheme,
    fontOptions,
    canFollowAppDefault = false,
    canResetToBuiltIn = false,
    isSaving,
    onSave,
}: WebWidgetThemeEditorProps) {
    const isFollowingAppDefault = canFollowAppDefault && theme === null;

    const savedTheme = theme ?? defaultTheme;

    const { form, isDirty, previewTheme, saveTheme, discardChanges, resetSection } = useThemeEditorForm({
        savedTheme,
        onSave,
    });

    // One state drives both the colors tabs and the previewed scheme, so the colors on screen are always
    // the colors in the frame and editing dark never means guessing.
    const [previewAppearance, setPreviewAppearance] = useState<PreviewAppearance>(
        savedTheme.appearance === "Dark" ? "Dark" : "Light",
    );
    const [previewView, setPreviewView] = useState<PreviewView>("Conversation");

    return (
        <form
            // flex-1 (not h-full) makes it explicit that the form fills whatever height its host page's
            // other flex siblings (header, description, save-error alert) leave behind, rather than relying
            // on the shrink algorithm to work that out from a hard-coded 100% height.
            className="flex min-h-0 flex-1 flex-col"
            onSubmit={form.handleSubmit(
                (submitted) => void saveTheme(toWidgetTheme(submitted)),
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
            <header className="flex flex-wrap items-center gap-3 border-b px-4 py-3">
                <div className="ml-auto flex flex-wrap items-center gap-2">
                    {canFollowAppDefault && !isFollowingAppDefault && (
                        <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            // Handing control back while the form is dirty would race the re-seed against
                            // markSaved and could baseline unsent edits as saved. Discard first.
                            disabled={isSaving || isDirty}
                            title="Discard your changes first"
                            onClick={() => void saveTheme(null)}
                        >
                            Follow app default
                        </Button>
                    )}
                    {canResetToBuiltIn && (
                        <Button
                            type="button"
                            variant="outline"
                            size="sm"
                            // Same gate as Follow app default: a null save while dirty could baseline unsent
                            // edits as saved. Discard first.
                            disabled={isSaving || isDirty}
                            title="Discard your changes first"
                            onClick={() => void saveTheme(null)}
                        >
                            Reset to built-in default
                        </Button>
                    )}
                    {isDirty && (
                        <Button type="button" variant="outline" size="sm" disabled={isSaving} onClick={discardChanges}>
                            Discard changes
                        </Button>
                    )}
                    <Button type="submit" size="sm" disabled={isSaving || !isDirty}>
                        {isSaving && <Spinner />}
                        Save
                    </Button>
                </div>
            </header>

            {isFollowingAppDefault && (
                <Alert className="mx-4 mt-4">
                    This widget follows the app-wide default. Change anything below and save to give it a theme of its
                    own.
                </Alert>
            )}

            <div className="grid min-h-0 flex-1 lg:grid-cols-[minmax(0,26rem)_minmax(0,1fr)]">
                <ThemeEditorInspector
                    control={form.control}
                    isSaving={isSaving}
                    onReset={resetSection}
                    fontOptions={fontOptions}
                    previewTheme={previewTheme}
                    previewAppearance={previewAppearance}
                    onPreviewAppearanceChange={setPreviewAppearance}
                    onFocusWelcomeFields={() => setPreviewView("Welcome")}
                />

                <ThemeEditorStage
                    previewTheme={previewTheme}
                    previewAppearance={previewAppearance}
                    onPreviewAppearanceChange={setPreviewAppearance}
                    previewView={previewView}
                    onPreviewViewChange={setPreviewView}
                />
            </div>
        </form>
    );
}
