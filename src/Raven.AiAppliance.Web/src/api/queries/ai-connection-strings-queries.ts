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
    };
}

export type AiConnectionStringsQueries = ReturnType<typeof createAiConnectionStringsQueries>;
