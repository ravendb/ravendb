import { useMemo, useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { ConversationDto } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { ConversationsTable } from "@/pages/apps/conversations/conversations-table";
import {
    ConversationsToolbar,
    type FilterOption,
    type StatusFilterOption,
} from "@/pages/apps/conversations/conversations-toolbar";
import { SectionCard } from "@/pages/apps/section-card";

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

const EMPTY_CONVERSATIONS: ConversationDto[] = [];

export function ConversationsSection({ slug }: { slug: string }) {
    const conversationsQuery = useQuery(api.queries.stats.conversations(slug));
    const conversations = conversationsQuery.data ?? EMPTY_CONVERSATIONS;

    const [search, setSearch] = useState("");
    const [status, setStatus] = useState("all");
    const [agent, setAgent] = useState("all");
    const [channel, setChannel] = useState("all");

    const { statusOptions, agentOptions, channelOptions } = useMemo(
        () => deriveFilterOptions(conversations),
        [conversations],
    );

    const filteredConversations = useMemo(
        () => filterConversations(conversations, { search, status, agent, channel }),
        [conversations, search, status, agent, channel],
    );

    return (
        <SectionCard title="Conversations" description="Live and historical chats across all channels.">
            <ApiState
                isLoading={conversationsQuery.isPending}
                isError={conversationsQuery.isError}
                errorTitle="Could not load conversations"
                onRetry={() => void conversationsQuery.refetch()}
                loadingLabel="Loading conversations..."
            >
                {conversationsQuery.data && (
                    <div className="space-y-4">
                        <ConversationsToolbar
                            search={search}
                            onSearchChange={setSearch}
                            status={status}
                            onStatusChange={setStatus}
                            statusOptions={statusOptions}
                            totalCount={conversations.length}
                            agent={agent}
                            onAgentChange={setAgent}
                            agentOptions={agentOptions}
                            channel={channel}
                            onChannelChange={setChannel}
                            channelOptions={channelOptions}
                        />
                        <ConversationsTable slug={slug} conversations={filteredConversations} />
                    </div>
                )}
            </ApiState>
        </SectionCard>
    );
}

// Active conversations surface first; completed/closed ones sink to the end. Unknown states land in
// the middle so the order stays stable if the backend introduces a new state.
const STATE_ORDER: Record<string, number> = { active: 0, idle: 1, completed: 3, closed: 3 };

function getStateOrder(value: string): number {
    return STATE_ORDER[value] ?? 2;
}

function deriveFilterOptions(conversations: ConversationDto[]) {
    const stateCounts = new Map<string, { label: string; count: number }>();
    const agents = new Set<string>();
    const channels = new Set<string>();

    for (const conversation of conversations) {
        // Skip blank values: a Select.Item (and a status pill) must have a non-empty value, and the
        // backend can return conversations with no agent/channel/state assigned yet.
        if (conversation.state.trim().length > 0) {
            const stateKey = conversation.state.toLowerCase();
            const existing = stateCounts.get(stateKey);
            if (existing) {
                existing.count += 1;
            } else {
                stateCounts.set(stateKey, { label: conversation.state, count: 1 });
            }
        }
        if (conversation.agentName.trim().length > 0) {
            agents.add(conversation.agentName);
        }
        if (conversation.channelName.trim().length > 0) {
            channels.add(conversation.channelName);
        }
    }

    const statusOptions: StatusFilterOption[] = [...stateCounts.entries()]
        .map(([value, { label, count }]) => ({ value, label, count }))
        .sort((a, b) => getStateOrder(a.value) - getStateOrder(b.value) || a.label.localeCompare(b.label));

    const toOptions = (values: Set<string>): FilterOption[] =>
        [...values].sort((a, b) => a.localeCompare(b)).map((value) => ({ value, label: value }));

    return { statusOptions, agentOptions: toOptions(agents), channelOptions: toOptions(channels) };
}

function filterConversations(
    conversations: ConversationDto[],
    filters: { search: string; status: string; agent: string; channel: string },
): ConversationDto[] {
    const query = filters.search.trim().toLowerCase();

    return conversations.filter((conversation) => {
        if (filters.status !== "all" && conversation.state.toLowerCase() !== filters.status) {
            return false;
        }
        if (filters.agent !== "all" && conversation.agentName !== filters.agent) {
            return false;
        }
        if (filters.channel !== "all" && conversation.channelName !== filters.channel) {
            return false;
        }
        if (query.length === 0) {
            return true;
        }

        const haystack = [
            conversation.id,
            conversation.channelName,
            conversation.agentName,
            ...conversation.params.flatMap((param) => [param.key, param.value]),
        ]
            .join(" ")
            .toLowerCase();
        return haystack.includes(query);
    });
}
