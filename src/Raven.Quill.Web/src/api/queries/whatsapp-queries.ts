import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "whatsapp";

export function createWhatsAppQueries(api: ServerApi["whatsapp"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: 30_000,
            }),
        pairing: (slug: string, channelId: string) =>
            queryOptions({
                queryKey: [baseKey, "pairing", slug, channelId],
                queryFn: () => api.pairing(slug, channelId),
                staleTime: 0,
            }),
    };
}

export type WhatsAppQueries = ReturnType<typeof createWhatsAppQueries>;
