import { queryOptions } from "@tanstack/react-query";
import type { DiscordChannelHealthResponse, ServerApi } from "@/api/generated/server-api";

const baseKey = "discord";

const CONNECTING_POLL_MS = 3_000;
const IDLE_POLL_MS = 30_000;
const CONNECTING_POLL_WINDOW_MS = 60_000;

// When each channel was first seen connecting. Per channel, not per app: a bot wedged in "Connecting..."
// must stop pulling the fast poll once its window is spent, without spending the window of a channel that
// starts connecting later.
const connectingSinceByChannel = new Map<string, number>();

function isAnyChannelWithinConnectingWindow(slug: string, rows: DiscordChannelHealthResponse[]) {
    const now = Date.now();
    const startedAt: number[] = [];

    for (const row of rows) {
        const key = `${slug}/${row.channelId}`;
        const isConnecting = row.enabled && !row.gatewayConnected && !row.lastGatewayError;

        if (isConnecting === false) {
            connectingSinceByChannel.delete(key);
            continue;
        }

        const since = connectingSinceByChannel.get(key) ?? now;
        connectingSinceByChannel.set(key, since);
        startedAt.push(since);
    }

    return startedAt.some((since) => now - since < CONNECTING_POLL_WINDOW_MS);
}

export function createDiscordQueries(api: ServerApi["discord"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: (query) =>
                    isAnyChannelWithinConnectingWindow(slug, query.state.data ?? [])
                        ? CONNECTING_POLL_MS
                        : IDLE_POLL_MS,
            }),
    };
}
