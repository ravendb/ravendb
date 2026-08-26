import { useId, useRef, useState } from "react";
import { createPortal } from "react-dom";
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
    /**
     * Where to render the Save/Discard group. The host page owns the header these belong in, so it hands
     * over the element to fill; without one they render in a bare row of their own above the fields.
     */
    actionsSlot?: HTMLElement | null;
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
    actionsSlot,
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
    const actionsRef = useRef<HTMLDivElement>(null);
    // Submit works by DOM ancestry, not by React tree, so Save needs to name the form it belongs to once
    // the actions are portalled into a header that sits outside it.
    const formId = useId();

    const onDiscardClick = () => {
        discardChanges();
        // Discard unmounts itself (it only renders while dirty) - without moving focus deliberately the
        // browser drops it to <body>, which for a keyboard or screen-reader user reads as being bounced
        // to the top of the document with no announcement.
        // The Save button looks like the natural target, but it sits right next to Discard and Discard
        // clearing the form's dirty state disables Save in the very same update - a focused control that
        // becomes disabled is blurred straight back to <body> by Chrome, reproducing the bug. The actions
        // container is the nearest target that is never disabled, so focus lands there instead.
        actionsRef.current?.focus();
    };

    const actions = (
        <div
            ref={actionsRef}
            tabIndex={-1}
            role="group"
            aria-label="Theme actions"
            // tabIndex={-1} also makes this mouse-focusable (clicking the gap between buttons
            // focuses it silently), so the ring only needs to show up for keyboard focus - a
            // screen reader still gets the announcement either way from role/aria-label above.
            className="ml-auto flex flex-wrap items-center gap-2 rounded-sm focus-visible:ring-2 focus-visible:ring-ring/50 focus-visible:outline-none"
        >
            {canFollowAppDefault && !isFollowingAppDefault && (
                <Button
                    type="button"
                    variant="outline"
                    size="sm"
                    // Handing control back while the form is dirty would race the re-seed against
                    // markSaved and could baseline unsent edits as saved. Discard first.
                    disabled={isSaving || isDirty}
                    title={isDirty ? "Discard your changes first" : undefined}
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
                    title={isDirty ? "Discard your changes first" : undefined}
                    onClick={() => void saveTheme(null)}
                >
                    Reset to built-in default
                </Button>
            )}
            {isDirty && (
                <Button type="button" variant="outline" size="sm" disabled={isSaving} onClick={onDiscardClick}>
                    Discard changes
                </Button>
            )}
            <Button type="submit" form={formId} size="sm" disabled={isSaving || !isDirty}>
                {isSaving && <Spinner />}
                Save
            </Button>
        </div>
    );

    return (
        <form
            id={formId}
            // flex-1 (not h-full) makes it explicit that the form fills whatever height its host page's
            // other flex siblings (the header and the save-error alert) leave behind, rather than relying
            // on the shrink algorithm to work that out from a hard-coded 100% height.
            // @container/theme-editor lets the inspector/stage split on the width this form actually gets
            // (its own container) instead of the viewport - see the @5xl/theme-editor: variants below.
            className="@container/theme-editor flex min-h-0 flex-1 flex-col"
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
            {/* No border/vertical padding of its own: the host page's header + description sit directly
                above this, and a second bordered, padded bar read as a half-finished toolbar. */}
            {actionsSlot ? (
                createPortal(actions, actionsSlot)
            ) : (
                /* No border or vertical padding of its own: this is the fallback for a host with no header
                   to hand over, and a second bordered, padded bar read as a half-finished toolbar. */
                <header className="flex flex-wrap items-center gap-3 px-4">{actions}</header>
            )}

            {isFollowingAppDefault && (
                <Alert className="mx-4 mt-4">
                    This widget follows the app-wide default. Change anything below and save to give it a theme of its
                    own.
                </Alert>
            )}

            {/* Below the two-pane breakpoint this stacks with natural height and the page scrolls once;
                the bounded, split layout only kicks in once the container actually has room for it. */}
            <div className="flex flex-col gap-4 @5xl/theme-editor:grid @5xl/theme-editor:min-h-0 @5xl/theme-editor:flex-1 @5xl/theme-editor:grid-cols-[minmax(0,26rem)_minmax(0,1fr)] @5xl/theme-editor:gap-0">
                <ThemeEditorInspector
                    control={form.control}
                    isSaving={isSaving}
                    onReset={resetSection}
                    fontOptions={fontOptions}
                    previewTheme={previewTheme}
                    previewAppearance={previewAppearance}
                    onPreviewAppearanceChange={setPreviewAppearance}
                    onFocusWelcomeFields={() => setPreviewView("Welcome")}
                    savedColors={{ Light: savedTheme.light, Dark: savedTheme.dark }}
                    defaultColors={{ Light: defaultTheme.light, Dark: defaultTheme.dark }}
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
