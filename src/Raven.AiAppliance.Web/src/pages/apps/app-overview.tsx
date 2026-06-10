import { useParams } from "react-router";
import { AgentsSection } from "@/pages/apps/agents-section";
import { ChannelsSection } from "@/pages/apps/channels-section";

export function AppOverview() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-5">
            <AgentsSection slug={slug} />
            <ChannelsSection slug={slug} />
        </div>
    );
}
