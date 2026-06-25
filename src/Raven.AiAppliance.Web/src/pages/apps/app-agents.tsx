import { useParams } from "react-router";
import { useQuery } from "@tanstack/react-query";
import { api } from "@/api/api";
import { RawDataPreview } from "@/components/data/raw-data-preview";
import { AgentsSection } from "@/pages/apps/agents-section";

export function AppAgents() {
    const { slug = "" } = useParams();
    const agentStatsQuery = useQuery(api.queries.stats.agents(slug));

    return (
        <div className="space-y-5">
            <AgentsSection slug={slug} />
            <RawDataPreview title="stats.agents" query={agentStatsQuery} />
        </div>
    );
}
