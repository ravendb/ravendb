import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { api } from "@/api/api";
import type { LicenseResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/shadcn/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { cn } from "@/lib/utils";
import { AddAiConnectionString } from "@/components/ai-connection-string/add-ai-connection-string";
import { EditAiConnectionString } from "@/components/ai-connection-string/edit-ai-connection-string";
import { CONNECTOR_TYPE_LABELS, MODEL_TYPE_LABELS } from "@/components/ai-connection-string/ai-connection-string-utils";
import { DeleteAiConnectionStringDialog } from "@/pages/apps/settings/delete-ai-connection-string-dialog";

export function AppSettings() {
    const { slug = "" } = useParams();
    const licenseQuery = useQuery(api.queries.settings.license());

    return (
        <div className="space-y-8">
            <AiConnectionStringsSection slug={slug} />
            <section className="space-y-4">
                <h2 className="text-sm font-semibold">License</h2>
                <ApiState
                    isLoading={licenseQuery.isPending}
                    isError={licenseQuery.isError}
                    errorTitle="Could not load license"
                    onRetry={() => void licenseQuery.refetch()}
                    loadingLabel="Loading license..."
                >
                    {licenseQuery.data && <LicenseSummaryCard license={licenseQuery.data} />}
                </ApiState>
            </section>
        </div>
    );
}

function LicenseSummaryCard({ license }: { license: LicenseResponse }) {
    const isExpired = license.state === "expired";
    const isTrial = license.tier === "Trial";
    const tierVariant = isExpired ? "destructive" : isTrial ? "warning" : "success";

    return (
        <Card>
            <CardHeader>
                <CardTitle className="flex items-center gap-2">
                    <Badge variant={tierVariant}>{license.tier}</Badge>
                    {isTrial && !isExpired && license.daysLeft > 0 && (
                        <span className="text-sm font-normal text-muted-foreground">
                            {license.daysLeft} {license.daysLeft === 1 ? "day" : "days"} left
                        </span>
                    )}
                </CardTitle>
                <CardDescription>Refreshed {license.lastRefreshedLabel}</CardDescription>
                <CardAction>
                    <Button asChild variant="outline" size="sm">
                        <Link to="/license">Manage license</Link>
                    </Button>
                </CardAction>
            </CardHeader>
            <CardContent className="grid gap-6 sm:grid-cols-3">
                <HealthMetric label="API" healthy={license.apiHealthy} value={license.api} />
                <HealthMetric
                    label="Connectivity"
                    healthy={license.connectivityOK}
                    value={license.connectivityOK ? "OK" : "Unavailable"}
                />
                <div className="space-y-1">
                    <div className="text-xs text-muted-foreground">Trial ends</div>
                    <div className="text-sm font-medium">{license.trialEndsLabel}</div>
                </div>
            </CardContent>
        </Card>
    );
}

function HealthMetric({ label, healthy, value }: { label: string; healthy: boolean; value: string }) {
    return (
        <div className="space-y-1">
            <div className="text-xs text-muted-foreground">{label}</div>
            <div className="flex items-center gap-2 text-sm font-medium">
                <span
                    className={cn("size-2 rounded-full", healthy ? "bg-emerald-500" : "bg-red-500")}
                    aria-hidden="true"
                />
                {value}
            </div>
        </div>
    );
}

function AiConnectionStringsSection({ slug }: { slug: string }) {
    const connectionStringsQuery = useQuery(api.queries.aiConnectionStrings.list(slug));
    const items = connectionStringsQuery.data?.items ?? [];

    const refetch = () => void connectionStringsQuery.refetch();

    return (
        <section>
            <div className="mb-4 flex items-start justify-between gap-3">
                <div className="space-y-1">
                    <h2 className="text-sm font-semibold">AI connection strings</h2>
                    <p className="text-sm text-muted-foreground">
                        Provider credentials your agents use to reach a model.
                    </p>
                </div>
                <AddAiConnectionString
                    slug={slug}
                    modelType="Chat"
                    onCreated={refetch}
                    trigger={
                        <Button size="sm" variant="outline">
                            <Plus className="size-3.5" aria-hidden="true" />
                            Add connection string
                        </Button>
                    }
                />
            </div>

            <ApiState
                isLoading={connectionStringsQuery.isPending}
                isError={connectionStringsQuery.isError}
                errorTitle="Could not load connection strings"
                onRetry={refetch}
                loadingLabel="Loading connection strings..."
            >
                <div className="overflow-hidden rounded-lg border">
                    <Table>
                        <TableHeader>
                            <TableRow className="hover:bg-transparent">
                                <TableHead className="text-xs font-medium text-muted-foreground">Name</TableHead>
                                <TableHead className="text-xs font-medium text-muted-foreground">Provider</TableHead>
                                <TableHead className="text-xs font-medium text-muted-foreground">Model type</TableHead>
                                <TableHead className="w-0 text-right text-xs font-medium text-muted-foreground">
                                    <span className="sr-only">Actions</span>
                                </TableHead>
                            </TableRow>
                        </TableHeader>
                        <TableBody>
                            {items.length === 0 ? (
                                <TableRow className="hover:bg-transparent">
                                    <TableCell colSpan={4} className="h-20 text-center text-muted-foreground">
                                        No connection strings yet.
                                    </TableCell>
                                </TableRow>
                            ) : (
                                items.map((item) => (
                                    <TableRow key={item.name}>
                                        <TableCell className="font-medium">{item.name}</TableCell>
                                        <TableCell>{CONNECTOR_TYPE_LABELS[item.provider]}</TableCell>
                                        <TableCell>
                                            <Badge variant="secondary">{MODEL_TYPE_LABELS[item.modelType]}</Badge>
                                        </TableCell>
                                        <TableCell className="text-right">
                                            <div className="flex justify-end gap-1">
                                                <EditAiConnectionString
                                                    slug={slug}
                                                    name={item.name}
                                                    modelType={item.modelType}
                                                    onSaved={refetch}
                                                    trigger={
                                                        <Button
                                                            variant="ghost"
                                                            size="icon-sm"
                                                            aria-label={`Edit ${item.name}`}
                                                        >
                                                            <Pencil className="size-3.5" aria-hidden="true" />
                                                        </Button>
                                                    }
                                                />
                                                <DeleteAiConnectionStringDialog
                                                    slug={slug}
                                                    name={item.name}
                                                    trigger={
                                                        <Button
                                                            variant="ghost"
                                                            size="icon-sm"
                                                            aria-label={`Delete ${item.name}`}
                                                        >
                                                            <Trash2 className="size-3.5" aria-hidden="true" />
                                                        </Button>
                                                    }
                                                />
                                            </div>
                                        </TableCell>
                                    </TableRow>
                                ))
                            )}
                        </TableBody>
                    </Table>
                </div>
            </ApiState>
        </section>
    );
}
