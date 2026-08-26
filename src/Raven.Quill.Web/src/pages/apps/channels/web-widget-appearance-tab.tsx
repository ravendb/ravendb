import { useQuery } from "@tanstack/react-query";
import { useState } from "react";
import { Text } from "@/components/typography";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { Alert } from "@/components/shadcn/ui/alert";
import { useWebWidgetThemeSave } from "@/pages/apps/channels/use-web-widget-theme-save";
import { WebWidgetThemeEditor } from "@/pages/apps/channels/theme-editor/theme-editor";

// The web-widget theme editor, rendered inside the channel detail's "Customize appearance" tab.
// Only mounted for web-widget (IFrame) channels, whose theme endpoints exist.
export function WebWidgetAppearanceTab({ slug, channelId }: { slug: string; channelId: string }) {
    // The editor's Save/Discard group belongs on this tab's own header row, so hand it the element to fill.
    const [actionsSlot, setActionsSlot] = useState<HTMLDivElement | null>(null);

    const themeQuery = useQuery(api.queries.webWidget.theme(slug, channelId));

    const saveMutation = useWebWidgetThemeSave({
        save: (theme) => api.services.iframe.updateTheme(slug, channelId, { theme }),
        invalidateKeys: [api.queries.webWidget.theme(slug, channelId).queryKey],
        successMessage: "Theme saved",
    });

    return (
        // The editor's inspector is its own scrolling region and its preview stage fills what is left, so
        // this tab hands it a bounded flex column rather than a scroller of its own.
        <div className="flex min-h-0 flex-1 flex-col gap-5">
            <div className="flex flex-wrap items-start justify-between gap-3">
                <Text variant="muted">
                    Choose how this web widget looks and reads. Every change previews live, and nothing reaches visitors
                    until you save.
                </Text>
                <div ref={setActionsSlot} className="flex shrink-0 flex-wrap items-center gap-2" />
            </div>

            {/* Held above the editor rather than inside it: it describes what saving will do to this
                widget, which is the decision the header's Save button is about. */}
            {themeQuery.data?.theme === null && (
                <Alert>
                    This widget follows the app-wide default. Change anything below and save to give it a theme of its
                    own.
                </Alert>
            )}

            <ApiState
                isLoading={themeQuery.isPending}
                isError={themeQuery.isError}
                errorTitle="Could not load the theme"
                onRetry={themeQuery.refetch}
                loadingLabel="Loading theme..."
                skeleton={<FormFieldsSkeleton count={4} />}
            >
                {themeQuery.data && (
                    <WebWidgetThemeEditor
                        theme={themeQuery.data.theme}
                        defaultTheme={themeQuery.data.defaultTheme}
                        fontOptions={themeQuery.data.fontOptions}
                        canFollowAppDefault
                        isSaving={saveMutation.isPending}
                        actionsSlot={actionsSlot}
                        onSave={(theme) => saveMutation.mutateAsync(theme)}
                    />
                )}
            </ApiState>

            {saveMutation.isError && (
                <Alert variant="destructive">
                    {saveMutation.error instanceof Error ? saveMutation.error.message : "Could not save the theme."}
                </Alert>
            )}
        </div>
    );
}
