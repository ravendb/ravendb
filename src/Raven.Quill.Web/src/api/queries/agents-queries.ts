import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "agents";

export function createAgentsQueries(api: ServerApi["agents"]) {
    return {
        list: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "list", slug],
                queryFn: () => api.list(slug),
            }),
        detail: (slug: string, agentId: string) =>
            queryOptions({
                queryKey: [baseKey, "detail", slug, agentId],
                queryFn: () => api.get(slug, agentId),
            }),
    };
}

export type AgentsQueries = ReturnType<typeof createAgentsQueries>;
