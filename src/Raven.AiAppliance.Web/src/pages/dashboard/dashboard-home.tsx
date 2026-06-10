import { Link } from "react-router";
import { Database, Plus } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { AgentSummaryResponse, AppResponse, ChannelSummaryResponse } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { PagePanel } from "@/components/data/page-panel";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { Skeleton } from "@/components/shadcn/ui/skeleton";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { cn } from "@/lib/utils";

export function DashboardHome() {
    const appsQuery = useQuery(api.queries.apps.list());

    return (
        <PagePanel>
            <ApiState
                isLoading={appsQuery.isPending}
                isError={appsQuery.isError}
                errorTitle="Could not load apps"
                onRetry={appsQuery.refetch}
                loadingLabel="Loading apps..."
            >
                {appsQuery.data && appsQuery.data.length > 0 ? (
                    <div className="space-y-4">
                        <div className="flex items-center justify-between gap-3">
                            <div className="flex items-center gap-2">
                                <h2 className="text-sm font-semibold">Apps</h2>
                                <Badge variant="secondary" className="font-mono">
                                    {appsQuery.data.length}
                                </Badge>
                            </div>
                            <Button asChild size="sm">
                                <Link to={appRoutes.addApp()}>
                                    <Plus className="size-3.5" aria-hidden="true" />
                                    Add app
                                </Link>
                            </Button>
                        </div>
                        <div className="overflow-hidden rounded-lg border">
                            <Table>
                                <TableHeader>
                                    <TableRow className="hover:bg-transparent">
                                        <TableHead className="w-[30%] text-xs font-medium text-muted-foreground">
                                            App
                                        </TableHead>
                                        <TableHead className="text-xs font-medium text-muted-foreground">
                                            Agents
                                        </TableHead>
                                        <TableHead className="text-xs font-medium text-muted-foreground">
                                            Channels
                                        </TableHead>
                                        <TableHead className="text-xs font-medium text-muted-foreground">
                                            Created
                                        </TableHead>
                                        <TableHead className="w-[22%] text-xs font-medium text-muted-foreground">
                                            Status
                                        </TableHead>
                                    </TableRow>
                                </TableHeader>
                                <TableBody>
                                    {appsQuery.data.map((app) => (
                                        <AppRow key={app.slug} app={app} />
                                    ))}
                                </TableBody>
                            </Table>
                        </div>
                    </div>
                ) : (
                    <EmptyAppsState />
                )}
            </ApiState>
        </PagePanel>
    );
}

function AppRow({ app }: { app: AppResponse }) {
    const agentsQuery = useQuery(api.queries.agents.list(app.slug));
    const channelsQuery = useQuery(api.queries.channels.list(app.slug));

    return (
        <TableRow className="group">
            <TableCell className="py-3">
                <Link to={appRoutes.app(app.slug)} className="flex flex-col gap-0.5">
                    <span className="text-sm font-medium group-hover:underline">{app.name}</span>
                    <span className="font-mono text-xs text-muted-foreground">{app.database}</span>
                </Link>
            </TableCell>
            <TableCell className="text-sm tabular-nums">
                {agentsQuery.isPending ? (
                    <Skeleton className="h-4 w-6" />
                ) : agentsQuery.data && agentsQuery.data.length > 0 ? (
                    agentsQuery.data.length
                ) : (
                    "—"
                )}
            </TableCell>
            <TableCell className="text-sm">
                {channelsQuery.isPending ? <Skeleton className="h-4 w-24" /> : formatChannels(channelsQuery.data)}
            </TableCell>
            <TableCell className="text-sm text-muted-foreground">{formatCreatedDate(app.createdAt)}</TableCell>
            <TableCell className="py-3">
                {agentsQuery.isPending || channelsQuery.isPending ? (
                    <Skeleton className="h-4 w-20" />
                ) : agentsQuery.data && channelsQuery.data ? (
                    <AppStatusCell agents={agentsQuery.data} channels={channelsQuery.data} />
                ) : (
                    <StatusBlock
                        dotClassName="bg-muted-foreground/50"
                        label="Unknown"
                        subtitle="Could not load status"
                    />
                )}
            </TableCell>
        </TableRow>
    );
}

function formatChannels(channels: ChannelSummaryResponse[] | undefined) {
    if (!channels || channels.length === 0) {
        return "—";
    }
    const labels = new Set(channels.map((channel) => (channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "Other")));
    return [...labels].join(", ");
}

function AppStatusCell({ agents, channels }: { agents: AgentSummaryResponse[]; channels: ChannelSummaryResponse[] }) {
    if (agents.length === 0) {
        return <StatusBlock dotClassName="bg-sky-500" label="Setup" subtitle="No AI capability yet" />;
    }
    if (agents.every((agent) => agent.disabled)) {
        return <StatusBlock dotClassName="bg-muted-foreground/50" label="Disabled" subtitle="All agents disabled" />;
    }
    if (channels.length > 0 && channels.every((channel) => !channel.enabled)) {
        return <StatusBlock dotClassName="bg-amber-500" label="Needs attention" subtitle="All channels disabled" />;
    }
    return <StatusBlock dotClassName="bg-emerald-500" label="Active" />;
}

function StatusBlock({ dotClassName, label, subtitle }: { dotClassName: string; label: string; subtitle?: string }) {
    return (
        <div className="flex items-start gap-2">
            <span className={cn("mt-1.5 size-1.5 shrink-0 rounded-full", dotClassName)} aria-hidden="true" />
            <div className="flex flex-col">
                <span className="text-sm font-medium">{label}</span>
                {subtitle && <span className="text-xs text-muted-foreground">{subtitle}</span>}
            </div>
        </div>
    );
}

function formatCreatedDate(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleDateString();
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
                        Add app
                    </Link>
                </Button>
            </div>
        </div>
    );
}
