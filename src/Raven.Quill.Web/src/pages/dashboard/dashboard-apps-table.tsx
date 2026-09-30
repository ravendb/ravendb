import type { ReactNode } from "react";
import { Link } from "react-router";
import { Database, Pencil, Plus, Trash2 } from "lucide-react";
import type { ApplianceAppResponse, AppWrites } from "@/api/generated/server-api";
import { CountBadge } from "@/components/data/count-badge";
import { StatusIndicator } from "@/components/data/status-indicator";
import { resolveStatusStyle } from "@/lib/app-status";
import { Button } from "@/components/shadcn/ui/button";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { TableSkeletonRows } from "@/components/table/table-skeleton";
import { WruLabel } from "@/components/data/wru-label";
import { appRoutes } from "@/lib/app-routes";
import { datePeriodUnit, type DatePeriod } from "@/lib/date-period";
import { formatCompact } from "@/lib/format";
import { DeleteAppDialog } from "@/pages/apps/delete-app-dialog";
import { EditAppConfirmDialog } from "@/pages/setup/add-app-wizard/edit-app-confirm-dialog";
import { Heading, Text } from "@/components/typography";
import { SectionHeader } from "@/components/section-header";

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
            <SectionHeader
                title="Apps"
                count={<CountBadge>{apps.length}</CountBadge>}
                action={
                    <Button asChild size="sm">
                        <Link to={appRoutes.addApp()}>
                            <Plus className="size-3.5" aria-hidden="true" />
                            Add app
                        </Link>
                    </Button>
                }
            />
            <AppsTableFrame period={period}>
                {apps.map((app) => (
                    <AppRow key={app.slug} app={app} writes={writesBySlug.get(app.slug)} />
                ))}
            </AppsTableFrame>
        </div>
    );
}

export function DashboardAppsTableSkeleton({ period }: { period: DatePeriod }) {
    return (
        <div className="space-y-4">
            {/* The button is drawn rather than rendered: `ApiState` hides the skeleton from the
                accessibility tree, and a real link in there would still take focus. */}
            <SectionHeader
                title="Apps"
                count={<Skeleton className="h-5 w-8 rounded-md" />}
                action={<Skeleton className="h-8 w-36 rounded-md" />}
            />
            <AppsTableFrame period={period}>
                <TableSkeletonRows columnCount={APPS_COLUMN_COUNT} rows={4} hasActionColumn />
            </AppsTableFrame>
        </div>
    );
}

const APPS_COLUMN_COUNT = 7;

function AppsTableFrame({ period, children }: { period: DatePeriod; children: ReactNode }) {
    return (
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
                <TableBody>{children}</TableBody>
            </Table>
        </div>
    );
}

function AppRow({ app, writes }: { app: ApplianceAppResponse; writes: number | undefined }) {
    return (
        <TableRow className="group">
            <TableCell className="py-3">
                <Link to={appRoutes.app(app.slug)} className="flex flex-col gap-0.5">
                    <Text as="span" variant="label" className="group-hover:underline">
                        {app.name}
                    </Text>
                    <Text as="span" variant="caption" className="font-mono">
                        {app.slug}
                    </Text>
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
                    <EditAppConfirmDialog
                        slug={app.slug}
                        trigger={
                            <Button variant="ghost" size="icon-sm" aria-label={`Edit ${app.name}`}>
                                <Pencil className="size-3.5" aria-hidden="true" />
                            </Button>
                        }
                    />
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
        <div className="flex flex-col items-start gap-1">
            <StatusIndicator tone={style.tone} label={style.label} />
            {app.statusSubtitle && (
                <Text as="span" variant="caption">
                    {app.statusSubtitle}
                </Text>
            )}
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
                <Heading variant="label" className="mt-4">
                    No apps added yet
                </Heading>
                <Text variant="caption" className="mt-3 leading-5">
                    Create an app from a source database and a table mapping.
                </Text>
                <Button asChild size="sm" className="mt-5">
                    <Link to={appRoutes.addApp()}>
                        <Plus className="size-3.5" aria-hidden="true" />
                        Add app
                    </Link>
                </Button>
            </div>
        </div>
    );
}
