import { useParams } from "react-router";
import { AgentsSection } from "@/pages/apps/agents-section";
import { ChannelsSection } from "@/pages/apps/channels-section";
import { StatisticsSection } from "@/pages/apps/statistics-section";
import { SyncHealthCard } from "@/pages/apps/sync-health-card";
import { WelcomePanel } from "@/pages/apps/welcome-panel";

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-8">
            <WelcomePanel slug={slug} />
            <SyncHealthCard slug={slug} />
            <StatisticsSection slug={slug} />
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
        </div>
    );
}
