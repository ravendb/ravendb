import { useParams } from "react-router";
import { AgentsSection } from "@/pages/apps/agents-section";
import { ChannelsSection } from "@/pages/apps/channels-section";
import { WelcomePanel } from "@/pages/apps/welcome-panel";

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-5">
            <WelcomePanel slug={slug} />
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
        </div>
    );
}
