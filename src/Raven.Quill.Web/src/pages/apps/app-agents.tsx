import { useParams } from "react-router";
import { AddAgentButton, AgentsTable } from "@/pages/apps/agents-section";
import { Heading } from "@/components/typography";

export function AppAgents() {
    const { slug = "" } = useParams();

    return (
        <div className="space-y-6">
            <div className="flex items-start justify-between gap-3">
                <Heading as="h1" variant="page">
                    Agents
                </Heading>
                <AddAgentButton slug={slug} />
            </div>
            <AgentsTable slug={slug} />
        </div>
    );
}
