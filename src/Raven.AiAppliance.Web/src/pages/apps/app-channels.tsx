import { Palette } from "lucide-react";
import { Link, useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { Button } from "@/components/shadcn/ui/button";
import { appRoutes } from "@/lib/app-routes";
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
        <div className="space-y-8">
            <div className="flex items-start justify-between gap-4">
                <p className="max-w-prose text-sm text-muted-foreground">
                    Channels are the surfaces end users reach your agents through.
                </p>
                <div className="flex shrink-0 items-center gap-2">
                    <Button asChild variant="outline">
                        <Link to={appRoutes.app(slug, "web-widget/default-customize")}>
                            <Palette aria-hidden="true" />
                            Edit default appearance
                        </Link>
                    </Button>
                    <AddChannelMenu slug={slug} label="New channel" />
                </div>
            </div>
            <DashboardStatCards cards={cards} />
            <ChannelGroups slug={slug} />
        </div>
    );
}
