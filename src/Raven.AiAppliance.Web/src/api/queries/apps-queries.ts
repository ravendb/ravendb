import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "apps";

export function createAppsQueries(api: ServerApi["apps"]) {
    return {
        cdcPerformance: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "cdcPerformance", slug],
                queryFn: () => api.cdcPerformance(slug),
            }),
        detail: (slug: string) =>
            queryOptions({
                queryKey: [baseKey, "detail", slug],
                queryFn: () => api.detail(slug),
            }),
        list: () =>
            queryOptions({
                queryKey: [baseKey, "list"],
                queryFn: () => api.list(),
            }),
    };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
