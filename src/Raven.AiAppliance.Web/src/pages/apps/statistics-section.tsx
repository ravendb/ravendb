import { useState } from "react";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { WindowTabs, type WindowKey } from "@/components/data/window-tabs";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { SectionCard } from "@/pages/apps/section-card";

export function StatisticsSection({ slug }: { slug: string }) {
    const [windowKey, setWindowKey] = useState<WindowKey>("last7d");
    const conversationStatsQuery = useQuery(api.queries.stats.conversationStats(slug));
    const dashboardAppQuery = useQuery(api.queries.stats.dashboardApp(slug));

    const windowData = conversationStatsQuery.data?.[windowKey];

    const cards: DashboardStatCard[] = [
        {
            label: "Conversations",
            value: windowData?.conversations,
            isLoading: conversationStatsQuery.isPending,
        },
        {
            label: "Messages",
            value: windowData?.messages,
            isLoading: conversationStatsQuery.isPending,
        },
        {
            label: "Tokens",
            value: windowData?.tokens,
            isLoading: conversationStatsQuery.isPending,
        },
        {
            label: "Writes",
            value: dashboardAppQuery.data?.writesPerMonth ?? undefined,
            isLoading: dashboardAppQuery.isPending,
            caption: "This month",
        },
    ];

    return (
        <SectionCard title="Statistics" action={<WindowTabs value={windowKey} onChange={setWindowKey} />}>
            <DashboardStatCards cards={cards} />
        </SectionCard>
    );
}
