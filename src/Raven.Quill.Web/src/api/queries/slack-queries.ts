import { queryOptions } from "@tanstack/react-query";
import type { ServerApi, SlackChannelHealthResponse } from "@/api/generated/server-api";
import { createConnectingPoll } from "@/api/queries/connecting-poll";

const baseKey = "slack";

const pollInterval = createConnectingPoll<SlackChannelHealthResponse>(baseKey, (row) => ({
    channelId: row.channelId,
    enabled: row.enabled,
    connected: row.socketConnected,
    error: row.lastSocketError,
}));

export function createSlackQueries(api: ServerApi["slack"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: (query) => pollInterval(slug, query.state.data ?? []),
            }),
    };
}

export type SlackQueries = ReturnType<typeof createSlackQueries>;
