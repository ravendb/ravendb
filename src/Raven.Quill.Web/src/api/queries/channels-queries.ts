import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "channels";

export function createChannelsQueries(api: ServerApi["channels"]) {
    return {
        list: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "list", slug],
                queryFn: () => api.list(slug),
            }),
    };
}

export type ChannelsQueries = ReturnType<typeof createChannelsQueries>;
