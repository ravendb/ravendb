import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import type {
    AppCdcConfigurationResponse,
    ApplianceAppResponse,
    MigrationPlanGetResponse,
    WizardDraftResponse,
} from "@/api/generated/server-api";
import { api } from "@/api/api";
import { isApiError } from "@/api/http-client";
import { ApiState } from "@/components/data/api-state";
import { FormFieldsSkeleton } from "@/components/data/loading-skeletons";
import { Alert, AlertDescription, AlertTitle } from "@/components/shadcn/ui/alert";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";
import { AppWizard } from "@/pages/setup/add-app-wizard/app-wizard";
import { buildEditAppSeed } from "@/pages/setup/add-app-wizard/edit-app-values";

export function EditAppWizard() {
    const { slug = "" } = useParams();
    // The dashboard row carries the name and source type, the CDC response the mapping and its source.
    const appQuery = useQuery(api.queries.stats.dashboardApp(slug));
    const configurationQuery = useQuery(api.queries.apps.cdcGet(slug));
    // Not kept between visits: an apply or a manual save since then decides whether the plan still applies.
    const draftQuery = useQuery({ ...api.queries.setup.draft(slug), gcTime: 0 });
    const hasNoDraft = isApiError(draftQuery.error) && draftQuery.error.status === 404;
    const shouldReadPlan = !!draftQuery.data?.hasPlan && !!draftQuery.data.appliedPlanVersions;
    const planQuery = useQuery({ ...api.queries.setup.migrationPlan(slug), gcTime: 0, enabled: shouldReadPlan });
    const isPlanSettled = hasNoDraft || (draftQuery.isSuccess && (!shouldReadPlan || planQuery.isSuccess));

    // The wizard fills the bare layout, so it must not sit inside a padded state container.
    if (appQuery.data && configurationQuery.data && isPlanSettled) {
        return (
            <EditAppWizardForm
                app={appQuery.data}
                cdc={configurationQuery.data}
                draft={draftQuery.data ?? null}
                plan={planQuery.data ?? null}
            />
        );
    }

    return (
        <div className="p-8">
            <ApiState
                isLoading={
                    appQuery.isPending ||
                    configurationQuery.isPending ||
                    draftQuery.isPending ||
                    (shouldReadPlan && planQuery.isPending)
                }
                isError={
                    appQuery.isError ||
                    configurationQuery.isError ||
                    (draftQuery.isError && !hasNoDraft) ||
                    planQuery.isError
                }
                errorTitle="Could not load the app configuration"
                loadingLabel="Loading the app configuration..."
                skeleton={<FormFieldsSkeleton count={4} />}
                onRetry={() => {
                    void appQuery.refetch();
                    void configurationQuery.refetch();
                    void draftQuery.refetch();

                    if (shouldReadPlan) {
                        void planQuery.refetch();
                    }
                }}
            >
                {null}
            </ApiState>
        </div>
    );
}

// Mounted only once every response is in, so the wizard seeds itself exactly once.
function EditAppWizardForm({
    app,
    cdc,
    draft,
    plan,
}: {
    app: ApplianceAppResponse;
    cdc: AppCdcConfigurationResponse;
    draft: WizardDraftResponse | null;
    plan: MigrationPlanGetResponse | null;
}) {
    const seed = buildEditAppSeed(app, cdc, draft, plan);

    if ("error" in seed) {
        return (
            <div className="p-8">
                <Alert variant="destructive" className="max-w-2xl">
                    <AlertTitle>This app&rsquo;s mapping cannot be edited here</AlertTitle>
                    <AlertDescription>
                        {seed.error}
                        <div className="mt-3">
                            <Button asChild type="button" variant="outline" size="sm">
                                <Link to={appRoutes.app(app.slug)} className="!no-underline">
                                    Back to the app
                                </Link>
                            </Button>
                        </div>
                    </AlertDescription>
                </Alert>
            </div>
        );
    }

    return <AppWizard defaultValues={seed.values} editedApp={seed.editedApp} />;
}
