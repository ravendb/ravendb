import { queryOptions } from "@tanstack/react-query";
import type { DiscordChannelHealthResponse, ServerApi } from "@/api/generated/server-api";
import { createConnectingPoll } from "@/api/queries/connecting-poll";

const baseKey = "discord";

const pollInterval = createConnectingPoll<DiscordChannelHealthResponse>(baseKey, (row) => ({
    channelId: row.channelId,
    enabled: row.enabled,
    connected: row.gatewayConnected,
    error: row.lastGatewayError,
}));

export function createDiscordQueries(api: ServerApi["discord"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: (query) => pollInterval(slug, query.state.data ?? []),
            }),
    };
}
