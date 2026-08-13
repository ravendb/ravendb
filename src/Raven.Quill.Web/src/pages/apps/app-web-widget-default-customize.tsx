import { useQuery } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { Link, useParams } from "react-router";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Alert } from "@/components/shadcn/ui/alert";
import { appRoutes } from "@/lib/app-routes";
import { useWebWidgetThemeSave } from "@/pages/apps/channels/use-web-widget-theme-save";
import { WebWidgetThemeEditor } from "@/pages/apps/channels/web-widget-theme-editor";

export function AppWebWidgetDefaultCustomize() {
    const { slug = "" } = useParams();

    const defaultQuery = useQuery(api.queries.webWidget.defaultTheme(slug));

    const saveMutation = useWebWidgetThemeSave({
        save: (theme) => api.services.iframe.updateDefaultTheme(slug, { theme }),
        invalidateKeys: [
            api.queries.webWidget.defaultTheme(slug).queryKey,
            // Each widget's theme response embeds this default as its fallback (defaultTheme), so the saved
            // default must also refresh their cached responses, not just this page's query.
            api.queries.webWidget.themesKey(slug),
        ],
        successMessage: "Default theme saved",
    });

    return (
        <div className="grid gap-5">
            <Link
                to={appRoutes.app(slug, "channels")}
                className="inline-flex w-fit items-center gap-1.5 text-sm text-muted-foreground transition-colors hover:text-foreground"
            >
                <ArrowLeft className="size-3.5" aria-hidden="true" />
                Back to channels
            </Link>

            <ApiState
                isLoading={defaultQuery.isPending}
                isError={defaultQuery.isError}
                errorTitle="Could not load the default theme"
                onRetry={() => defaultQuery.refetch()}
                loadingLabel="Loading default theme..."
            >
                <p className="text-sm text-muted-foreground">
                    This theme applies to every web widget that doesn&rsquo;t have one of its own.
                </p>

                {defaultQuery.data && (
                    <WebWidgetThemeEditor
                        theme={defaultQuery.data.theme}
                        defaultTheme={defaultQuery.data.theme}
                        fontOptions={defaultQuery.data.fontOptions}
                        isSaving={saveMutation.isPending}
                        onSave={(theme) => saveMutation.mutate(theme)}
                    />
                )}

                {saveMutation.isError && (
                    <Alert variant="destructive">
                        {saveMutation.error instanceof Error
                            ? saveMutation.error.message
                            : "Could not save the default theme."}
                    </Alert>
                )}
            </ApiState>
        </div>
    );
}
