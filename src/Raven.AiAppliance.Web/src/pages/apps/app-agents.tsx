import { useParams } from "react-router";
import { AgentsSection } from "@/pages/apps/agents-section";

export function AppAgents() {
    const { slug = "" } = useParams();

    return <AgentsSection slug={slug} />;
}
