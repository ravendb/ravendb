import { useMutation, useQuery, useQueryClient } from "@tanstack/react-query";
import { ArrowLeft } from "lucide-react";
import { Link, useParams } from "react-router";
import { toast } from "sonner";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Alert } from "@/components/shadcn/ui/alert";
import { appRoutes } from "@/lib/app-routes";
import { WebWidgetStyleEditor } from "@/pages/apps/channels/web-widget-style-editor";

export function AppWebWidgetDefaultCustomize() {
    const { slug = "" } = useParams();
    const queryClient = useQueryClient();

    const defaultQuery = useQuery(api.queries.webWidget.defaultCustomization(slug));
    const styleGuideQuery = useQuery(api.queries.webWidget.styleGuide(slug));
    const previewQuery = useQuery(api.queries.webWidget.preview(slug));

    const saveMutation = useMutation({
        mutationFn: (css: string) => api.services.iframe.updateDefaultCustomization(slug, { css: css || null }),
        onSuccess: async () => {
            await queryClient.invalidateQueries({
                queryKey: api.queries.webWidget.defaultCustomization(slug).queryKey,
            });
            toast.success("Default styles saved");
        },
    });

    const onRetry = async () => {
        if (defaultQuery.isError) await defaultQuery.refetch();
        if (styleGuideQuery.isError) await styleGuideQuery.refetch();
    };

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
                isLoading={defaultQuery.isPending || styleGuideQuery.isPending}
                isError={defaultQuery.isError || styleGuideQuery.isError}
                errorTitle="Could not load default styles"
                onRetry={onRetry}
                loadingLabel="Loading default styles..."
            >
                <p className="text-sm text-muted-foreground">
                    These styles apply to every web widget that has no styles of its own. The editor below starts
                    pre-filled with the widget&rsquo;s base styles as a starting point.
                </p>

                {defaultQuery.data && styleGuideQuery.data && (
                    <WebWidgetStyleEditor
                        initialCss={defaultQuery.data.css ?? ""}
                        baseCss={styleGuideQuery.data.baseCss}
                        previewHtml={previewQuery.data?.html ?? ""}
                        isSaving={saveMutation.isPending}
                        onSave={(css) => saveMutation.mutate(css)}
                    />
                )}

                {saveMutation.isError && (
                    <Alert variant="destructive">
                        {saveMutation.error instanceof Error
                            ? saveMutation.error.message
                            : "Could not save default styles."}
                    </Alert>
                )}
            </ApiState>
        </div>
    );
}
