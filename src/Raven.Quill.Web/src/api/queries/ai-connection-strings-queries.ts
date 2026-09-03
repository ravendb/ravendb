import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "aiConnectionStrings";

export function createAiConnectionStringsQueries(api: ServerApi["aiConnectionStrings"]) {
    return {
        list: () =>
            queryOptions({
                queryKey: [baseKey, "list"],
                queryFn: () => api.list(),
            }),
        detail: (name: string) =>
            queryOptions({
                queryKey: [baseKey, "detail", name],
                queryFn: () => api.detail(name),
            }),
    };
}

export type AiConnectionStringsQueries = ReturnType<typeof createAiConnectionStringsQueries>;
