import { useMemo, useState } from "react";
import { keepPreviousData, useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import type { ConversationDto } from "@/api/generated/server-api";
import { ApiState } from "@/components/data/api-state";
import { DatePeriodPicker } from "@/components/data/date-period-picker";
import { TablePagination } from "@/components/table/table-pagination";
import type { DatePeriod } from "@/lib/date-period";
import { StatCardsSection, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { ConversationsTable, ConversationsTableSkeleton } from "@/pages/apps/conversations/conversations-table";
import {
    ConversationsToolbar,
    type FilterOption,
    type StatusFilterOption,
} from "@/pages/apps/conversations/conversations-toolbar";
import { SectionCard } from "@/pages/apps/section-card";

interface ConversationsSectionProps {
    slug: string;
    period: DatePeriod;
}

export function ConversationStatsCards({
    slug,
    period,
    earliest,
    onPeriodChange,
}: ConversationsSectionProps & { earliest: Date | undefined; onPeriodChange: (value: DatePeriod) => void }) {
    const conversationStatsQuery = useQuery(api.queries.stats.conversationStats(slug, period));
    const stats = conversationStatsQuery.data;

    const cards: DashboardStatCard[] = [
        { label: "Conversations", value: stats?.conversations, isLoading: conversationStatsQuery.isPending },
        { label: "Prompts", value: stats?.messages, isLoading: conversationStatsQuery.isPending },
        { label: "Tokens", value: stats?.tokens, isLoading: conversationStatsQuery.isPending },
    ];

    return (
        <StatCardsSection
            cards={cards}
            action={<DatePeriodPicker value={period} earliest={earliest} onChange={onPeriodChange} />}
        />
    );
}

const EMPTY_CONVERSATIONS: ConversationDto[] = [];
const PAGE_SIZE = 50;

export function ConversationsSection({ slug, period }: ConversationsSectionProps) {
    const [pageIndex, setPageIndex] = useState(0);

    // A page number only makes sense within one period, so switching periods jumps back to the
    // first page. Adjusting state during render avoids fetching the stale page first.
    const periodKey = `${period.year}-${period.month ?? ""}-${period.day ?? ""}`;
    const [prevPeriodKey, setPrevPeriodKey] = useState(periodKey);
    if (periodKey !== prevPeriodKey) {
        setPrevPeriodKey(periodKey);
        setPageIndex(0);
    }

    const conversationsQuery = useQuery({
        ...api.queries.stats.conversations(slug, period, { start: pageIndex * PAGE_SIZE, pageSize: PAGE_SIZE }),
        placeholderData: keepPreviousData,
    });
    const conversations = conversationsQuery.data?.conversations ?? EMPTY_CONVERSATIONS;
    const totalResults = conversationsQuery.data?.totalResults ?? 0;

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

    const emptyMessage =
        conversations.length === 0 ? "No conversations yet." : "No conversations match the current filters.";

    return (
        <SectionCard>
            <ApiState
                isLoading={conversationsQuery.isPending}
                isError={conversationsQuery.isError}
                errorTitle="Could not load conversations"
                onRetry={() => void conversationsQuery.refetch()}
                loadingLabel="Loading conversations..."
                skeleton={<ConversationsTableSkeleton slug={slug} />}
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
                        <ConversationsTable
                            slug={slug}
                            conversations={filteredConversations}
                            emptyMessage={emptyMessage}
                        />
                        <TablePagination
                            pageIndex={pageIndex}
                            pageSize={PAGE_SIZE}
                            totalCount={totalResults}
                            onPageIndexChange={setPageIndex}
                        />
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
            ...conversation.lastExchange.map((turn) => turn.content ?? ""),
        ]
            .join(" ")
            .toLowerCase();
        return haystack.includes(query);
    });
}
