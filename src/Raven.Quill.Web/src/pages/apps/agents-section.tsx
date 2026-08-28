import { Link } from "react-router";
import { Pencil, Plus, Trash2 } from "lucide-react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { EnabledStatus } from "@/components/data/status-indicator";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { SectionTable, SectionTableSkeleton } from "@/components/table/section-table";
import { appRoutes } from "@/lib/app-routes";
import { formatCompact } from "@/lib/format";
import { formatDateTime } from "@/lib/utils";
import { DeleteAgentDialog } from "@/pages/apps/agents/delete-agent-dialog";
import { SectionCard } from "@/pages/apps/section-card";

const AGENT_TABLE_HEADERS = ["Agent name", "Status", "Model", "Last run", "Conversations", "Prompts", "Tokens", ""];

export function AgentsSection({ slug }: { slug: string }) {
    return (
        <SectionCard title="Agents" action={<AddAgentButton slug={slug} variant="outline" />}>
            <AgentsTable slug={slug} />
        </SectionCard>
    );
}

export function AgentsTable({ slug }: { slug: string }) {
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    return (
        <ApiState
            isLoading={agentsQuery.isPending}
            isError={agentsQuery.isError}
            errorTitle="Could not load agents"
            onRetry={() => void agentsQuery.refetch()}
            loadingLabel="Loading agents..."
            skeleton={<SectionTableSkeleton headers={AGENT_TABLE_HEADERS} />}
        >
            {agentsQuery.data && (
                <SectionTable
                    headers={AGENT_TABLE_HEADERS}
                    isEmpty={agentsQuery.data.length === 0}
                    emptyMessage="No agents yet."
                >
                    {agentsQuery.data.map((agent) => (
                        <TableRow key={agent.agentId}>
                            <TableCell className="font-medium">{agent.name}</TableCell>
                            <TableCell>
                                <EnabledStatus isEnabled={!agent.disabled} />
                            </TableCell>
                            <TableCell className="font-mono text-xs text-muted-foreground">
                                {agent.model ?? "—"}
                            </TableCell>
                            <TableCell className="whitespace-nowrap text-muted-foreground">
                                {agent.lastInvokedAt ? formatDateTime(agent.lastInvokedAt) : "—"}
                            </TableCell>
                            <TableCell className="tabular-nums">{formatCompact(agent.conversations)}</TableCell>
                            <TableCell className="tabular-nums">{formatCompact(agent.messages)}</TableCell>
                            <TableCell className="tabular-nums">{formatCompact(agent.tokens)}</TableCell>
                            <TableCell className="text-right">
                                <div className="flex items-center justify-end gap-1">
                                    <Button
                                        asChild
                                        variant="ghost"
                                        size="icon-sm"
                                        aria-label={`Edit ${agent.name}`}
                                        title="Edit agent"
                                    >
                                        <Link
                                            to={appRoutes.app(slug, `agents/${encodeURIComponent(agent.agentId)}/edit`)}
                                        >
                                            <Pencil className="size-3.5" aria-hidden="true" />
                                        </Link>
                                    </Button>
                                    <DeleteAgentDialog
                                        slug={slug}
                                        agent={agent}
                                        trigger={
                                            <Button
                                                variant="ghost"
                                                size="icon-sm"
                                                aria-label={`Delete ${agent.name}`}
                                                title="Delete agent"
                                            >
                                                <Trash2 className="size-3.5" aria-hidden="true" />
                                            </Button>
                                        }
                                    />
                                </div>
                            </TableCell>
                        </TableRow>
                    ))}
                </SectionTable>
            )}
        </ApiState>
    );
}

export function AddAgentButton({ slug, variant = "default" }: { slug: string; variant?: "default" | "outline" }) {
    return (
        <Button asChild variant={variant} size="sm">
            <Link to={appRoutes.addCapability(slug, "agent")}>
                <Plus className="size-3.5" aria-hidden="true" />
                Add agent
            </Link>
        </Button>
    );
}
