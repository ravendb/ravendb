import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "embed-links";

export function createEmbedLinksQueries(api: ServerApi["embedLinks"]) {
    return {
        list: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "list", slug],
                queryFn: () => api.list(slug),
            }),
    };
}

export type EmbedLinksQueries = ReturnType<typeof createEmbedLinksQueries>;
