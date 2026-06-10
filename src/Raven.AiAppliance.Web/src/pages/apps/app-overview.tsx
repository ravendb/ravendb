import { Link, useParams } from "react-router";
import { Plus } from "lucide-react";
import type { ReactNode } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { Button } from "@/components/shadcn/ui/button";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/shadcn/ui/table";
import { appRoutes } from "@/lib/app-routes";
import { CHANNEL_TYPE_LABELS } from "@/lib/channel-type-labels";
import { cn } from "@/lib/utils";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-5">
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
        </div>
    );
}

function AgentsSection({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    return (
        <SectionCard title="Agents" action={<AddAgentButton slug={slug} />}>
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
    const agentsQuery = useQuery(api.queries.agents.list(slug));
    const channelsQuery = useQuery(api.queries.channels.list(slug));

    const onRetry = async () => {
        if (channelsQuery.isError) {
            await channelsQuery.refetch();
        }
        if (agentsQuery.isError) {
            await agentsQuery.refetch();
        }
    };

    return (
        <SectionCard title="Channels" action={<AddChannelMenu slug={slug} />}>
            <ApiState
                isLoading={channelsQuery.isPending || agentsQuery.isPending}
                isError={channelsQuery.isError || agentsQuery.isError}
                errorTitle="Could not load channels"
                onRetry={onRetry}
                loadingLabel="Loading channels..."
            >
                {channelsQuery.data && (
                    <OverviewTable
                        headers={["Channel name", "Agent name", "Status", "Type", "Created", "Widget ID"]}
                        isEmpty={channelsQuery.data.length === 0}
                        emptyMessage="No channels yet."
                    >
                        {channelsQuery.data.map((channel) => (
                            <TableRow key={channel.widgetId}>
                                <TableCell className="font-medium">{channel.displayName}</TableCell>
                                <TableCell className="font-medium">
                                    {agentsQuery.data?.find((x) => x.agentId === channel.agentId)?.name}
                                </TableCell>
                                <TableCell>
                                    <StatusIndicator
                                        tone={channel.enabled ? "positive" : "muted"}
                                        label={channel.enabled ? "Connected" : "Disabled"}
                                    />
                                </TableCell>
                                <TableCell>{channel.type ? CHANNEL_TYPE_LABELS[channel.type] : "—"}</TableCell>
                                <TableCell className="text-muted-foreground">{formatDate(channel.createdAt)}</TableCell>
                                <TableCell className="text-muted-foreground">{channel.widgetId}</TableCell>
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

function AddAgentButton({ slug }: { slug: string }) {
    return (
        <Button asChild size="sm" variant="outline">
            <Link to={appRoutes.addCapability(slug, "agent")}>
                <Plus className="size-3.5" aria-hidden="true" />
                Add agent
            </Link>
        </Button>
    );
}

function formatDate(value: string) {
    const date = new Date(value);
    return Number.isNaN(date.getTime()) ? value : date.toLocaleString();
}
