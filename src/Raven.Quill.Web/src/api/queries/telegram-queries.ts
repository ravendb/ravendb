import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "telegram";

export function createTelegramQueries(api: ServerApi["telegram"]) {
    return {
        health: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "health", slug],
                queryFn: () => api.health(slug),
                refetchInterval: 30_000,
            }),
    };
}

export type TelegramQueries = ReturnType<typeof createTelegramQueries>;
