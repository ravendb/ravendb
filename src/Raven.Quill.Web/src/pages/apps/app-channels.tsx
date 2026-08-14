import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { DashboardStatCards, type DashboardStatCard } from "@/pages/dashboard/dashboard-stat-cards";
import { AddChannelMenu } from "@/pages/apps/channels/add-channel-menu";
import { ChannelGroups } from "@/pages/apps/channel-groups";

export function AppChannels() {
    const { slug = "" } = useParams();
    const channelStatsQuery = useQuery(api.queries.stats.channels(slug));

    const stats = channelStatsQuery.data;
    const cards: DashboardStatCard[] = [
        { label: "Active channels", value: stats?.active, isLoading: channelStatsQuery.isPending },
    ];

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <div className="space-y-1">
                    <h1 className="text-2xl font-semibold tracking-tight">Channels</h1>
                    <p className="max-w-prose text-sm text-muted-foreground">
                        Channels are the surfaces end users reach your agents through.
                    </p>
                </div>
                <AddChannelMenu slug={slug} label="New channel" variant="default" />
            </div>
            <DashboardStatCards cards={cards} />
            <ChannelGroups slug={slug} />
        </div>
    );
}
