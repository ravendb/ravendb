import { useParams } from "react-router";
import { AddAgentButton, AgentsTable } from "@/pages/apps/agents-section";

export function AppAgents() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <h1 className="text-2xl font-semibold tracking-tight">Agents</h1>
                <AddAgentButton slug={slug} />
            </div>
            <AgentsTable slug={slug} />
        </div>
    );
}
