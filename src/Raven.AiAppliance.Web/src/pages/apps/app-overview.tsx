import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { RawDataPreview } from "@/components/data/raw-data-preview";
import { AgentsSection } from "@/pages/apps/agents-section";
import { ChannelsSection } from "@/pages/apps/channels-section";
import { WelcomePanel } from "@/pages/apps/welcome-panel";

export function AppOverview() {
    const { slug = "" } = useParams();
    const overviewQuery = useQuery(api.queries.stats.overview(slug));
    const activityQuery = useQuery(api.queries.stats.activity(slug));
    const conversationStatsQuery = useQuery(api.queries.stats.conversationStats(slug));
    const dashboardAppQuery = useQuery(api.queries.stats.dashboardApp(slug));

    return (
        <div className="space-y-5">
            <WelcomePanel slug={slug} />
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
            <RawDataPreview title="stats.overview" query={overviewQuery} />
            <RawDataPreview title="stats.activity" query={activityQuery} />
            <RawDataPreview title="stats.conversationStats" query={conversationStatsQuery} />
            <RawDataPreview title="stats.dashboardApp" query={dashboardAppQuery} />
        </div>
    );
}
