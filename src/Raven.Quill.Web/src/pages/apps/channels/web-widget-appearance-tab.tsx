import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Alert } from "@/components/shadcn/ui/alert";
import { useWebWidgetThemeSave } from "@/pages/apps/channels/use-web-widget-theme-save";
import { WebWidgetThemeEditor } from "@/pages/apps/channels/web-widget-theme-editor";

// The web-widget theme editor, rendered inside the channel detail's "Customize appearance" tab.
// Only mounted for web-widget (IFrame) channels, whose theme endpoints exist.
export function WebWidgetAppearanceTab({ slug, channelId }: { slug: string; channelId: string }) {
    const themeQuery = useQuery(api.queries.webWidget.theme(slug, channelId));

    const saveMutation = useWebWidgetThemeSave({
        save: (theme) => api.services.iframe.updateTheme(slug, channelId, { theme }),
        invalidateKeys: [api.queries.webWidget.theme(slug, channelId).queryKey],
        successMessage: "Theme saved",
    });

    return (
        <div className="grid gap-5">
            <p className="text-sm text-muted-foreground">
                Choose how this web widget looks and reads. Pick an accent color and the rest of the palette is derived
                from it, so light and dark both stay coherent.
            </p>

            <ApiState
                isLoading={themeQuery.isPending}
                isError={themeQuery.isError}
                errorTitle="Could not load the theme"
                onRetry={themeQuery.refetch}
                loadingLabel="Loading theme..."
            >
                {themeQuery.data && (
                    <WebWidgetThemeEditor
                        theme={themeQuery.data.theme}
                        defaultTheme={themeQuery.data.defaultTheme}
                        fontOptions={themeQuery.data.fontOptions}
                        canFollowAppDefault
                        isSaving={saveMutation.isPending}
                        onSave={(theme) => saveMutation.mutate(theme)}
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
