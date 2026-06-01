import { Link } from "react-router";
import { Database, Plus } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { PagePanel } from "@/components/data/page-panel";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";

export function DashboardHome() {
    const appsQuery = useQuery(api.queries.apps.list());

    return (
        <PagePanel>
            <ApiState
                isLoading={appsQuery.isPending}
                isError={appsQuery.isError}
                errorTitle="Could not load apps"
                onRetry={() => void appsQuery.refetch()}
                loadingLabel="Loading apps..."
            >
                {appsQuery.data && appsQuery.data.length > 0 ? (
                    <div className="space-y-4">
                        <div className="flex items-center justify-between gap-3">
                            <h2 className="text-sm font-semibold">Available apps</h2>
                            <Button asChild size="sm">
                                <Link to={appRoutes.setupConnect()}>
                                    <Plus className="size-3.5" aria-hidden="true" />
                                    Add app
                                </Link>
                            </Button>
                        </div>
                        <div className="grid gap-3 md:grid-cols-2 xl:grid-cols-3">
                            {appsQuery.data.map((app) => (
                                <Link
                                    key={app.id}
                                    to={appRoutes.app(app.id)}
                                    className="rounded-lg border bg-background p-4 text-card-foreground transition-colors hover:bg-accent hover:text-accent-foreground"
                                >
                                    <div className="flex items-center gap-3">
                                        <div className="flex size-9 items-center justify-center rounded-md bg-accent text-accent-foreground">
                                            <Database className="size-5" aria-hidden="true" />
                                        </div>
                                        <div className="min-w-0">
                                            <p className="truncate text-sm font-semibold">{app.name}</p>
                                            <p className="truncate text-xs text-muted-foreground">{app.database}</p>
                                        </div>
                                    </div>
                                </Link>
                            ))}
                        </div>
                    </div>
                ) : (
                    <EmptyAppsState />
                )}
            </ApiState>
        </PagePanel>
    );
}

function EmptyAppsState() {
    return (
        <div className="flex min-h-full items-center justify-center">
            <div className="flex max-w-xs flex-col items-center text-center">
                <div className="flex size-9 items-center justify-center rounded-md bg-accent text-accent-foreground">
                    <Database className="size-5" aria-hidden="true" />
                </div>
                <h2 className="mt-4 text-sm font-semibold">No apps added yet</h2>
                <p className="mt-3 text-xs leading-5 text-muted-foreground">
                    Create an app from a source database and CDC mapping.
                </p>
                <Button asChild size="sm" className="mt-5">
                    <Link to={appRoutes.setupConnect()}>
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add app
                    </Link>
                </Button>
            </div>
        </div>
    );
}
