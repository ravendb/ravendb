import { Link } from "react-router";
import { Database, Pencil, Plus, Trash2 } from "lucide-react";
import type { ApplianceAppResponse, AppWrites } from "@/api/generated/server-api";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { WruLabel } from "@/components/data/wru-label";
import { appRoutes } from "@/lib/app-routes";
import { datePeriodUnit, type DatePeriod } from "@/lib/date-period";
import { formatCompact } from "@/lib/format";
import { cn } from "@/lib/utils";
import { DeleteAppDialog } from "@/pages/apps/delete-app-dialog";

type StatusStyle = { dotClassName: string; label: string };

// Maps the server's derived status codes (MetricsReadService.DeriveAppStatus emits
// running/warning/setup) onto the dashboard's status vocabulary. loading/failed are
// kept for forward-compatibility so a future provisioning/error state renders sensibly.
const STATUS_STYLES: Record<string, StatusStyle> = {
    running: { dotClassName: "bg-emerald-500", label: "Healthy" },
    healthy: { dotClassName: "bg-emerald-500", label: "Healthy" },
    warning: { dotClassName: "bg-amber-500", label: "Needs attention" },
    setup: { dotClassName: "bg-sky-500", label: "Setup" },
    loading: { dotClassName: "bg-muted-foreground/50", label: "Loading" },
    failed: { dotClassName: "bg-red-500", label: "Failed" },
    error: { dotClassName: "bg-red-500", label: "Failed" },
};

function resolveStatusStyle(status: string): StatusStyle {
    return (
        STATUS_STYLES[status.toLowerCase()] ?? {
            dotClassName: "bg-muted-foreground/50",
            label: status ? status[0].toUpperCase() + status.slice(1) : "Unknown",
        }
    );
}

export function DashboardAppsTable({
    apps,
    period,
    writesByApp,
}: {
    apps: ApplianceAppResponse[];
    period: DatePeriod;
    /** Per-app WRU totals for the selected period; undefined while the usage query loads. */
    writesByApp?: AppWrites[];
}) {
    if (apps.length === 0) {
        return <EmptyAppsState />;
    }

    const writesBySlug = new Map(writesByApp?.map((entry) => [entry.slug, entry.writes]));

    return (
        <div className="space-y-4">
            <div className="flex items-center justify-between gap-3">
                <div className="flex items-center gap-2">
                    <h2 className="text-lg font-semibold tracking-tight">Apps</h2>
                    <Badge variant="secondary" className="font-mono">
                        {apps.length}
                    </Badge>
                </div>
                <Button asChild size="sm">
                    <Link to={appRoutes.addApp()}>
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add application
                    </Link>
                </Button>
            </div>
            <div className="overflow-hidden rounded-lg border">
                <Table>
                    <TableHeader>
                        <TableRow className="hover:bg-transparent">
                            <TableHead className="w-[26%] text-xs font-medium text-muted-foreground">App</TableHead>
                            <TableHead className="text-xs font-medium text-muted-foreground">Source</TableHead>
                            <TableHead className="text-xs font-medium text-muted-foreground">Agents</TableHead>
                            <TableHead className="text-xs font-medium text-muted-foreground">Channels</TableHead>
                            <TableHead className="text-xs font-medium text-muted-foreground">
                                <WruLabel suffix={` / ${datePeriodUnit(period)}`} />
                            </TableHead>
                            <TableHead className="w-[20%] text-xs font-medium text-muted-foreground">Status</TableHead>
                            <TableHead className="w-0 text-right text-xs font-medium text-muted-foreground">
                                <span className="sr-only">Actions</span>
                            </TableHead>
                        </TableRow>
                    </TableHeader>
                    <TableBody>
                        {apps.map((app) => (
                            <AppRow key={app.slug} app={app} writes={writesBySlug.get(app.slug)} />
                        ))}
                    </TableBody>
                </Table>
            </div>
        </div>
    );
}

function AppRow({ app, writes }: { app: ApplianceAppResponse; writes: number | undefined }) {
    return (
        <TableRow className="group">
            <TableCell className="py-3">
                <Link to={appRoutes.app(app.slug)} className="flex flex-col gap-0.5">
                    <span className="text-sm font-medium group-hover:underline">{app.name}</span>
                    <span className="font-mono text-xs text-muted-foreground">{app.slug}</span>
                </Link>
            </TableCell>
            <TableCell className="text-sm">{app.source.type || "—"}</TableCell>
            <TableCell className="text-sm tabular-nums">{app.agentsCount > 0 ? app.agentsCount : "—"}</TableCell>
            <TableCell className="text-sm">{app.channelsLabel ?? "—"}</TableCell>
            <TableCell className="text-sm tabular-nums">{writes != null ? formatCompact(writes) : "—"}</TableCell>
            <TableCell className="py-3">
                <AppStatusCell app={app} />
            </TableCell>
            <TableCell className="text-right">
                <div className="flex items-center justify-end gap-1">
                    <Button asChild variant="ghost" size="icon-sm" aria-label={`Edit ${app.name}`}>
                        <Link to={appRoutes.editApp(app.slug)}>
                            <Pencil className="size-3.5" aria-hidden="true" />
                        </Link>
                    </Button>
                    <DeleteAppDialog
                        slug={app.slug}
                        appName={app.name}
                        trigger={
                            <Button variant="ghost" size="icon-sm" aria-label={`Delete ${app.name}`}>
                                <Trash2 className="size-3.5" aria-hidden="true" />
                            </Button>
                        }
                    />
                </div>
            </TableCell>
        </TableRow>
    );
}

function AppStatusCell({ app }: { app: ApplianceAppResponse }) {
    const style = resolveStatusStyle(app.status);

    return (
        <div className="flex items-start gap-2">
            <span className={cn("mt-1.5 size-1.5 shrink-0 rounded-full", style.dotClassName)} aria-hidden="true" />
            <div className="flex flex-col">
                <span className="text-sm font-medium">{style.label}</span>
                {app.statusSubtitle && <span className="text-xs text-muted-foreground">{app.statusSubtitle}</span>}
            </div>
        </div>
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
                    <Link to={appRoutes.addApp()}>
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add application
                    </Link>
                </Button>
            </div>
        </div>
    );
}
