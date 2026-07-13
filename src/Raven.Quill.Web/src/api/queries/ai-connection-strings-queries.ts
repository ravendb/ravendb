import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "aiConnectionStrings";

export function createAiConnectionStringsQueries(api: ServerApi["aiConnectionStrings"]) {
    return {
        list: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "list", slug],
                queryFn: () => api.list(slug),
            }),
        detail: (slug: string, name: string) =>
            queryOptions({
                queryKey: [baseKey, "detail", slug, name],
                queryFn: () => api.detail(slug, name),
            }),
    };
}

export type AiConnectionStringsQueries = ReturnType<typeof createAiConnectionStringsQueries>;
