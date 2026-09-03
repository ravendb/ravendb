import { useQuery } from "@tanstack/react-query";
import { useParams } from "react-router";
import { api } from "@/api/api";
import type { ExistingAgent } from "@/pages/setup/add-capability-wizard/capability-wizard-validation";

export function useExistingAgents(): ExistingAgent[] {
    const { slug = "" } = useParams();
    const agentsQuery = useQuery(api.queries.agents.list(slug));

    return agentsQuery.data ?? [];
}
