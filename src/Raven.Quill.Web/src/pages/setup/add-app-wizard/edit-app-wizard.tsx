import { useQuery } from "@tanstack/react-query";
import { Link, useParams } from "react-router";
import type { AppCdcConfigurationResponse, ApplianceAppResponse } from "@/api/generated/server-api";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
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

    // The wizard fills the bare layout, so it must not sit inside a padded state container.
    if (appQuery.data && configurationQuery.data) {
        return <EditAppWizardForm app={appQuery.data} cdc={configurationQuery.data} />;
    }

    return (
        <div className="p-8">
            <ApiState
                isLoading={appQuery.isPending || configurationQuery.isPending}
                isError={appQuery.isError || configurationQuery.isError}
                errorTitle="Could not load the application configuration"
                loadingLabel="Loading the application configuration..."
                onRetry={() => {
                    void appQuery.refetch();
                    void configurationQuery.refetch();
                }}
            >
                {null}
            </ApiState>
        </div>
    );
}

// Mounted only once both responses are in, so the wizard seeds itself exactly once.
function EditAppWizardForm({ app, cdc }: { app: ApplianceAppResponse; cdc: AppCdcConfigurationResponse }) {
    const seed = buildEditAppSeed(app, cdc);

    if ("error" in seed) {
        return (
            <div className="p-8">
                <Alert variant="destructive" className="max-w-2xl">
                    <AlertTitle>This application&rsquo;s mapping cannot be edited here</AlertTitle>
                    <AlertDescription>
                        {seed.error}
                        <div className="mt-3">
                            <Button asChild type="button" variant="outline" size="sm">
                                <Link to={appRoutes.app(app.slug)} className="!no-underline">
                                    Back to the application
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
