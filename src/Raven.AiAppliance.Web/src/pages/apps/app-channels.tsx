import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { ChannelsSection } from "@/pages/apps/channels-section";

export function AppChannels() {
    const { slug = "" } = useParams();
    const channelStatsQuery = useQuery(api.queries.stats.channels(slug));

    const stats = channelStatsQuery.data;
    const cards: DashboardStatCard[] = [
        { label: "Active channels", value: stats?.active ?? 0, isLoading: channelStatsQuery.isPending },
    ];

    return (
        <div className="space-y-8">
            <DashboardStatCards cards={cards} />
            <ChannelsSection slug={slug} />
        </div>
    );
}
