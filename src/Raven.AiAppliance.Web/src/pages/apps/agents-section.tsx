import { Link } from "react-router";
import { Plus } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { StatusIndicator } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { appRoutes } from "@/lib/app-routes";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function AgentsSection({ slug }: { slug: string }) {
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
                    <SectionTable
                        headers={["Agent name", "Status", "Model", "Runs", "Last run"]}
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
                                <TableCell className="tabular-nums">{formatCompact(agent.invocations)}</TableCell>
                                <TableCell className="whitespace-nowrap text-muted-foreground">
                                    {agent.lastInvokedAt ? formatDateTime(agent.lastInvokedAt) : "—"}
                                </TableCell>
                            </TableRow>
                        ))}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
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
