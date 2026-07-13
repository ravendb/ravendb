import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { Link, useParams } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Alert } from "@/components/shadcn/ui/alert";
import { appRoutes } from "@/lib/app-routes";
import { useWebWidgetStyleSave } from "@/pages/apps/channels/use-web-widget-style-save";
import { WebWidgetStyleEditor } from "@/pages/apps/channels/web-widget-style-editor";

export function AppWebWidgetCustomize() {
    const { slug = "", widgetId = "" } = useParams();

    const channelsQuery = useQuery(api.queries.channels.list(slug));
    const channel = channelsQuery.data?.find((candidate) => candidate.widgetId === widgetId);
    // Customization is web-widget-only, and the widget-scoped endpoints 404 for anything else. Gate the
    // widget-scoped queries on the channel being an iFrame so an unknown or non-iFrame widgetId resolves
    // to the not-found alert below instead of a generic "could not load" error from a request bound to fail.
    const isWebWidget = channel?.type === "IFrame";

    const customizationQuery = useQuery({
        ...api.queries.webWidget.customization(slug, widgetId),
        enabled: isWebWidget,
    });
    const styleGuideQuery = useQuery({ ...api.queries.webWidget.styleGuide(slug), enabled: isWebWidget });
    // Fetch the preview once we know the widget name so its header matches the live widget.
    const previewQuery = useQuery({
        ...api.queries.webWidget.preview(slug, channel?.displayName),
        enabled: isWebWidget,
    });

    const saveMutation = useWebWidgetStyleSave({
        save: (update) => api.services.iframe.updateCustomization(slug, widgetId, update),
        invalidateKeys: [api.queries.webWidget.customization(slug, widgetId).queryKey],
        successMessage: "Customization saved",
    });

    const onRetry = async () => {
        if (channelsQuery.isError) await channelsQuery.refetch();
        if (customizationQuery.isError) await customizationQuery.refetch();
        if (styleGuideQuery.isError) await styleGuideQuery.refetch();
        if (previewQuery.isError) await previewQuery.refetch();
    };

    return (
        <div className="grid gap-5">
            <Link
                to={appRoutes.app(slug, `channels/${encodeURIComponent(widgetId)}`)}
                className="inline-flex w-fit items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
                <ArrowLeft className="size-3.5" aria-hidden="true" />
                Back to channel
            </Link>

            <ApiState
                isLoading={
                    channelsQuery.isPending ||
                    (isWebWidget &&
                        (customizationQuery.isPending || styleGuideQuery.isPending || previewQuery.isPending))
                }
                isError={
                    channelsQuery.isError ||
                    customizationQuery.isError ||
                    styleGuideQuery.isError ||
                    previewQuery.isError
                }
                errorTitle="Could not load customization"
                onRetry={onRetry}
                loadingLabel="Loading customization..."
            >
                {!isWebWidget ? (
                    <Alert variant="destructive">No web widget “{widgetId}” in this app.</Alert>
                ) : (
                    <>
                        <div className="grid gap-1">
                            <h2 className="text-lg font-semibold">{channel.displayName}</h2>
                            <p className="text-sm text-muted-foreground">
                                Choose how this web widget&rsquo;s embed page looks: follow the app default, pick one of
                                the built-in styles, or write custom CSS over the widget&rsquo;s base styles.
                            </p>
                        </div>

                        {customizationQuery.data && styleGuideQuery.data && (
                            <WebWidgetStyleEditor
                                initialStyle={customizationQuery.data.style ?? null}
                                initialCss={customizationQuery.data.css ?? ""}
                                appDefault={{
                                    style: customizationQuery.data.defaultStyle ?? "Light",
                                    css: customizationQuery.data.defaultCss ?? "",
                                }}
                                baseCss={styleGuideQuery.data.baseCss}
                                lightThemeCss={styleGuideQuery.data.lightThemeCss}
                                darkThemeCss={styleGuideQuery.data.darkThemeCss}
                                previewHtml={previewQuery.data?.html ?? ""}
                                isSaving={saveMutation.isPending}
                                onSave={(update) => saveMutation.mutate(update)}
                            />
                        )}

                        {saveMutation.isError && (
                            <Alert variant="destructive">
                                {saveMutation.error instanceof Error
                                    ? saveMutation.error.message
                                    : "Could not save customization."}
                            </Alert>
                        )}
                    </>
                )}
            </ApiState>
        </div>
    );
}
