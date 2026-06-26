import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatCompact } from "@/lib/format";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function AgentUsageSection({ slug }: { slug: string }) {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");
    const agentStatsQuery = useQuery(api.queries.stats.agents(slug));
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    const stats = agentStatsQuery.data;
    const windowData = stats?.[windowKey];

    const cards: DashboardStatCard[] = [
        { label: "Conversations", value: windowData?.conversations, isLoading: agentStatsQuery.isPending },
        { label: "Messages", value: windowData?.messages, isLoading: agentStatsQuery.isPending },
        { label: "Tokens", value: windowData?.tokens, isLoading: agentStatsQuery.isPending },
    ];

    const agentNameById = new Map(agentsQuery.data?.map((agent) => [agent.agentId, agent.name]));

    return (
        <div className="space-y-8">
            <SectionCard title="Activity" action={<WindowTabs value={windowKey} onChange={setWindowKey} />}>
                <DashboardStatCards cards={cards} />
            </SectionCard>

            <SectionCard title="Activity by agent">
                <ApiState
                    isLoading={agentStatsQuery.isPending}
                    isError={agentStatsQuery.isError}
                    errorTitle="Could not load agent usage"
                    onRetry={() => void agentStatsQuery.refetch()}
                    loadingLabel="Loading agent usage..."
                >
                    {stats && (
                        <SectionTable
                            headers={["Agent", "Conversations", "Messages", "Tokens"]}
                            isEmpty={stats.agents.length === 0}
                            emptyMessage="No agent usage yet."
                        >
                            {stats.agents.map((agent) => (
                                <TableRow key={agent.agentId}>
                                    <TableCell className="font-medium">
                                        {agentNameById.get(agent.agentId) ?? agent.agentId}
                                    </TableCell>
                                    <TableCell className="tabular-nums">{formatCompact(agent.conversations)}</TableCell>
                                    <TableCell className="tabular-nums">{formatCompact(agent.messages)}</TableCell>
                                    <TableCell className="tabular-nums">{formatCompact(agent.tokens)}</TableCell>
                                </TableRow>
                            ))}
                        </SectionTable>
                    )}
                </ApiState>
            </SectionCard>
        </div>
    );
}
