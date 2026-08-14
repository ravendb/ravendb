import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { Link, useParams } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Alert } from "@/components/shadcn/ui/alert";
import { appRoutes } from "@/lib/app-routes";
import { useWebWidgetThemeSave } from "@/pages/apps/channels/use-web-widget-theme-save";
import { WebWidgetThemeEditor } from "@/pages/apps/channels/web-widget-theme-editor";

export function AppWebWidgetCustomize() {
    const { slug = "", channelId = "" } = useParams();

    const channelsQuery = useQuery(api.queries.channels.list(slug));
    const channel = channelsQuery.data?.find((candidate) => candidate.channelId === channelId);
    // Theming is web-widget-only, and the widget-scoped endpoints 404 for anything else. Gate the widget-scoped
    // query on the channel being an iFrame so an unknown or non-iFrame channelId resolves to the not-found alert
    // below instead of a generic "could not load" error from a request bound to fail.
    const isWebWidget = channel?.type === "IFrame";

    const themeQuery = useQuery({ ...api.queries.webWidget.theme(slug, channelId), enabled: isWebWidget });

    const saveMutation = useWebWidgetThemeSave({
        save: (theme) => api.services.iframe.updateTheme(slug, channelId, { theme }),
        invalidateKeys: [api.queries.webWidget.theme(slug, channelId).queryKey],
        successMessage: "Theme saved",
    });

    const onRetry = async () => {
        if (channelsQuery.isError) await channelsQuery.refetch();
        if (themeQuery.isError) await themeQuery.refetch();
    };

    return (
        <div className="grid gap-5">
            <Link
                to={appRoutes.app(slug, `channels/${encodeURIComponent(channelId)}`)}
                className="inline-flex w-fit items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
                <ArrowLeft className="size-3.5" aria-hidden="true" />
                Back to channel
            </Link>

            <ApiState
                isLoading={channelsQuery.isPending || (isWebWidget && themeQuery.isPending)}
                isError={channelsQuery.isError || themeQuery.isError}
                errorTitle="Could not load the theme"
                onRetry={onRetry}
                loadingLabel="Loading theme..."
            >
                {!isWebWidget ? (
                    <Alert variant="destructive">No web widget &ldquo;{channelId}&rdquo; in this app.</Alert>
                ) : (
                    <>
                        <div className="grid gap-1">
                            <h2 className="text-lg font-semibold">{channel.displayName}</h2>
                            <p className="text-sm text-muted-foreground">
                                Choose how this web widget looks and reads. Pick an accent color and the rest of the
                                palette is derived from it, so light and dark both stay coherent.
                            </p>
                        </div>

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

                        {saveMutation.isError && (
                            <Alert variant="destructive">
                                {saveMutation.error instanceof Error
                                    ? saveMutation.error.message
                                    : "Could not save the theme."}
                            </Alert>
                        )}
                    </>
                )}
            </ApiState>
        </div>
    );
}
