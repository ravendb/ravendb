import { Link, useParams } from "react-router";
import { Plus, Sparkles } from "lucide-react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { ChannelType } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@/components/shadcn/ui/tooltip";
import { appRoutes } from "@/lib/app-routes";
import { cn } from "@/lib/utils";

const CHANNEL_TYPE_LABELS: Record<NonNullable<ChannelType>, string> = {
    IFrame: "iFrame",
    Telegram: "Telegram",
    WhatsApp: "WhatsApp",
};

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <TooltipProvider>
            <div className="space-y-5">
                <div className="flex items-center justify-end">
                    <Button asChild size="sm">
                        <Link to={appRoutes.addCapability(slug)}>
                            <Sparkles className="size-3.5" aria-hidden="true" />
                            Add AI Capability
                        </Link>
                    </Button>
                </div>

                <AgentsSection slug={slug} />
                <ChannelsSection slug={slug} />
            </div>
        </TooltipProvider>
    );
}

function AgentsSection({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    return (
        <SectionCard title="Agents" action={<DisabledAddButton label="Add agent" />}>
            <ApiState
                isLoading={agentsQuery.isPending}
                isError={agentsQuery.isError}
                errorTitle="Could not load agents"
                onRetry={() => void agentsQuery.refetch()}
                loadingLabel="Loading agents..."
            >
                {agentsQuery.data && (
                    <OverviewTable
                        headers={["Agent name", "Status", "Model"]}
                        isEmpty={agentsQuery.data.length === 0}
                        emptyMessage="No agents yet."
                    >
                        {agentsQuery.data.map((agent) => (
                            <TableRow key={agent.agentId}>
                                <TableCell className="font-medium">{agent.name}</TableCell>
                                <TableCell>
                                    <StatusIndicator
                                        tone={agent.disabled ? "muted" : "positive"}
                                        label={agent.disabled ? "Disabled" : "Active"}
                                    />
                                </TableCell>
                                <TableCell className="font-mono text-xs text-muted-foreground">
                                    {agent.model ?? "—"}
                                </TableCell>
                            </TableRow>
                        ))}
                    </OverviewTable>
                )}
            </ApiState>
        </SectionCard>
    );
}

function ChannelsSection({ slug }: { slug: string }) {
    const channelsQuery = useQuery(api.queries.channels.list(slug));

    return (
        <SectionCard title="Channels" action={<DisabledAddButton label="Add channel" />}>
            <ApiState
                isLoading={channelsQuery.isPending}
                isError={channelsQuery.isError}
                errorTitle="Could not load channels"
                onRetry={() => void channelsQuery.refetch()}
                loadingLabel="Loading channels..."
            >
                {channelsQuery.data && (
                    <OverviewTable
                        headers={["Channel name", "Status", "Type", "Created"]}
                        isEmpty={channelsQuery.data.length === 0}
                        emptyMessage="No channels yet."
                    >
                        {channelsQuery.data.map((channel) => (
                            <TableRow key={channel.widgetId}>
                                <TableCell className="font-medium">{channel.displayName}</TableCell>
                                <TableCell>
                                    <StatusIndicator
                                        tone={channel.enabled ? "positive" : "muted"}
                                        label={channel.enabled ? "Connected" : "Disabled"}
                                    />
                                </TableCell>
                                <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                <TableCell className="text-muted-foreground">{formatDate(channel.createdAt)}</TableCell>
                            </TableRow>
                        ))}
                    </OverviewTable>
                )}
            </ApiState>
        </SectionCard>
    );
}

function SectionCard({ title, action, children }: { title: string; action?: ReactNode; children: ReactNode }) {
    return (
        <section>
            <div className="mb-4 flex items-center justify-between gap-3">
                <h2 className="text-sm font-semibold">{title}</h2>
                {action}
            </div>
            {children}
        </section>
    );
}

function OverviewTable({
    headers,
    isEmpty,
    emptyMessage,
    children,
}: {
    headers: string[];
    isEmpty: boolean;
    emptyMessage: string;
    children: ReactNode;
}) {
    return (
        <div className="overflow-hidden rounded-lg border">
            <Table>
                <TableHeader>
                    <TableRow className="hover:bg-transparent">
                        {headers.map((header) => (
                            <TableHead key={header} className="text-xs font-medium text-muted-foreground">
                                {header}
                            </TableHead>
                        ))}
                    </TableRow>
                </TableHeader>
                <TableBody>
                    {isEmpty ? (
                        <TableRow className="hover:bg-transparent">
                            <TableCell colSpan={headers.length} className="h-20 text-center text-muted-foreground">
                                {emptyMessage}
                            </TableCell>
                        </TableRow>
                    ) : (
                        children
                    )}
                </TableBody>
            </Table>
        </div>
    );
}

function StatusIndicator({ tone, label }: { tone: "positive" | "muted"; label: string }) {
    return (
        <span
            className={cn(
                "inline-flex items-center gap-1.5 rounded-full px-2 py-0.5 text-xs font-medium",
                tone === "positive"
                    ? "bg-emerald-500/10 text-emerald-600 dark:text-emerald-400"
                    : "bg-muted text-muted-foreground",
            )}
        >
            <span
                className={cn(
                    "size-1.5 rounded-full",
                    tone === "positive" ? "bg-emerald-500" : "bg-muted-foreground/50",
                )}
                aria-hidden="true"
            />
            {label}
        </span>
    );
}

function DisabledAddButton({ label }: { label: string }) {
    return (
        <Tooltip>
            <TooltipTrigger asChild>
                <span tabIndex={0} className="inline-flex cursor-not-allowed">
                    <Button size="sm" variant="outline" disabled aria-disabled="true">
                        <Plus className="size-3.5" aria-hidden="true" />
                        {label}
                    </Button>
                </span>
            </TooltipTrigger>
            <TooltipContent>Create from the “Add AI Capability” wizard.</TooltipContent>
        </Tooltip>
    );
}

function formatDate(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
