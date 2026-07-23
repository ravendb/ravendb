import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Trash2 } from "lucide-react";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { ConnectivityMetric } from "@/components/data/connectivity-metric";
import { Button } from "@/components/shadcn/ui/button";
import type { ConnectivityStatus, ServerLicenseResponse } from "@/api/generated/server-api";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { formatDate } from "@/lib/format";
import { getLicenseDaysLeft } from "@/lib/license";
import { DeleteAppDialog } from "@/pages/apps/delete-app-dialog";

export function AppSettings() {
    const { slug = "" } = useParams();
    const licenseQuery = useQuery(api.queries.settings.license());

    return (
        <div className="space-y-8">
            <section className="space-y-4">
                <h2 className="text-sm font-semibold">License</h2>
                <ApiState
                    isLoading={licenseQuery.isPending}
                    isError={licenseQuery.isError}
                    errorTitle="Could not load license"
                    onRetry={() => void licenseQuery.refetch()}
                    loadingLabel="Loading license..."
                >
                    {licenseQuery.data && (
                        <LicenseSummaryCard
                            license={licenseQuery.data.response}
                            connectivity={licenseQuery.data.connectivity}
                        />
                    )}
                </ApiState>
            </section>
            <DangerZoneSection slug={slug} />
        </div>
    );
}

function DangerZoneSection({ slug }: { slug: string }) {
    const appQuery = useQuery(api.queries.apps.detail(slug));
    const app = appQuery.data;

    if (!app) {
        return null;
    }

    return (
        <section className="space-y-4">
            <h2 className="text-sm font-semibold">Danger zone</h2>
            <Card className="border-destructive/50">
                <CardHeader>
                    <CardTitle>Delete this app</CardTitle>
                    <CardDescription>
                        Permanently removes "{app.name}" along with its agents, channels, and conversations.
                    </CardDescription>
                    <CardAction>
                        <DeleteAppDialog
                            slug={slug}
                            appName={app.name}
                            trigger={
                                <Button variant="destructive" size="sm">
                                    <Trash2 className="size-3.5" aria-hidden="true" />
                                    Delete app
                                </Button>
                            }
                        />
                    </CardAction>
                </CardHeader>
            </Card>
        </section>
    );
}

function LicenseSummaryCard({
    license,
    connectivity,
}: {
    license: ServerLicenseResponse;
    connectivity: ConnectivityStatus;
}) {
    const daysLeft = getLicenseDaysLeft(license);

    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    {daysLeft > 0 && (
                        <span className="text-sm font-normal text-muted-foreground">
                            {daysLeft} {daysLeft === 1 ? "day" : "days"} left
                        </span>
                    )}
                </CardTitle>
                <CardDescription>
                    {license.expired ? "Expired" : "Expires"} {formatDate(license.expiration)}
                </CardDescription>
                <CardAction>
                    <Button asChild variant="outline" size="sm">
                        <Link to="/license">Manage license</Link>
                    </Button>
                </CardAction>
            </CardHeader>
            <CardContent>
                <ConnectivityMetric connectivity={connectivity} />
            </CardContent>
        </Card>
    );
}
