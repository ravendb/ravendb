import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { ApiState } from "@/components/data/api-state";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { Badge } from "@/components/shadcn/ui/badge";
import { Button } from "@/components/shadcn/ui/button";
import { TableCell, TableRow } from "@/components/shadcn/ui/table";
import { formatDateTime } from "@/lib/utils";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { ConversationTranscriptSheet } from "@/pages/apps/conversations/conversation-transcript-sheet";
import { SectionCard, SectionTable } from "@/pages/apps/section-card";

export function ConversationStatsCards({ slug }: { slug: string }) {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");
    const conversationStatsQuery = useQuery(api.queries.stats.conversationStats(slug));
    const windowData = conversationStatsQuery.data?.[windowKey];

    const cards: DashboardStatCard[] = [
        { label: "Conversations", value: windowData?.conversations, isLoading: conversationStatsQuery.isPending },
        { label: "Messages", value: windowData?.messages, isLoading: conversationStatsQuery.isPending },
        { label: "Tokens", value: windowData?.tokens, isLoading: conversationStatsQuery.isPending },
    ];

    return (
        <SectionCard title="Activity" action={<WindowTabs value={windowKey} onChange={setWindowKey} />}>
            <DashboardStatCards cards={cards} />
        </SectionCard>
    );
}

export function ConversationsSection({ slug }: { slug: string }) {
    const conversationsQuery = useQuery(api.queries.stats.conversations(slug));

    return (
        <SectionCard title="Conversations">
            <ApiState
                isLoading={conversationsQuery.isPending}
                isError={conversationsQuery.isError}
                errorTitle="Could not load conversations"
                onRetry={() => void conversationsQuery.refetch()}
                loadingLabel="Loading conversations..."
            >
                {conversationsQuery.data && (
                    <SectionTable
                        headers={["Agent", "Channel", "State", "Last activity", ""]}
                        isEmpty={conversationsQuery.data.length === 0}
                        emptyMessage="No conversations yet."
                    >
                        {conversationsQuery.data.map((conversation) => (
                            <TableRow key={conversation.id}>
                                <TableCell>
                                    <div className="flex items-center gap-2 font-medium">
                                        <span
                                            className="flex size-6 shrink-0 items-center justify-center rounded-full text-xs font-medium text-white"
                                            style={{ backgroundColor: conversation.agentColor }}
                                            aria-hidden="true"
                                        >
                                            {conversation.agentInitials}
                                        </span>
                                        {conversation.agentName}
                                    </div>
                                </TableCell>
                                <TableCell>{conversation.channelName}</TableCell>
                                <TableCell>
                                    <Badge
                                        variant={
                                            conversation.state.toLowerCase() === "active" ? "success" : "secondary"
                                        }
                                    >
                                        {conversation.state}
                                    </Badge>
                                </TableCell>
                                <TableCell className="whitespace-nowrap text-muted-foreground">
                                    {formatDateTime(conversation.lastActivityAt)}
                                </TableCell>
                                <TableCell className="text-right">
                                    <ConversationTranscriptSheet
                                        slug={slug}
                                        conversationId={conversation.id}
                                        agentName={conversation.agentName}
                                        channelName={conversation.channelName}
                                        trigger={
                                            <Button variant="ghost" size="sm">
                                                View
                                            </Button>
                                        }
                                    />
                                </TableCell>
                            </TableRow>
                        ))}
                    </SectionTable>
                )}
            </ApiState>
        </SectionCard>
    );
}
