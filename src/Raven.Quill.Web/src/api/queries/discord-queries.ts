import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "discord";

export function createDiscordQueries(api: ServerApi["discord"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: (query) =>
                    query.state.data?.some((row) => row.enabled && !row.gatewayConnected && !row.lastGatewayError)
                        ? 3_000
                        : 30_000,
            }),
    };
}

export type DiscordQueries = ReturnType<typeof createDiscordQueries>;
