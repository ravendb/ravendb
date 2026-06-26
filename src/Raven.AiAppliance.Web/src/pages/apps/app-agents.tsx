import { useParams } from "react-router";
import { AgentUsageSection } from "@/pages/apps/agent-usage-section";
import { AgentsSection } from "@/pages/apps/agents-section";

export function AppAgents() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-8">
            <AgentUsageSection slug={slug} />
            <AgentsSection slug={slug} />
        </div>
    );
}
