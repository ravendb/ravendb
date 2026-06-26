import { useParams } from "react-router";
import { ActivitySection } from "@/pages/apps/activity-section";
import { AgentsSection } from "@/pages/apps/agents-section";
import { ChannelsSection } from "@/pages/apps/channels-section";
import { StatisticsSection } from "@/pages/apps/statistics-section";
import { WelcomePanel } from "@/pages/apps/welcome-panel";

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-8">
            <WelcomePanel slug={slug} />
            <StatisticsSection slug={slug} />
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
            <ActivitySection slug={slug} />
        </div>
    );
}
