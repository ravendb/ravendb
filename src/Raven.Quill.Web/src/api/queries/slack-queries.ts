import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";
import { MS_IN } from "@/lib/time";

const baseKey = "slack";

export function createSlackQueries(api: ServerApi["slack"]) {
    return {
        webhookInfo: (slug: string, channelId: string) =>
            queryOptions({
                queryKey: [baseKey, "webhook-info", slug, channelId],
                queryFn: () => api.webhookInfo(slug, channelId),
            }),
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: 30 * MS_IN.second,
            }),
    };
}

export type SlackQueries = ReturnType<typeof createSlackQueries>;
