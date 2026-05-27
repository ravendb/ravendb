import { queryOptions } from "@tanstack/react-query";
import type { ServerApi } from "@/api/generated/server-api";

const baseKey = "bootstrap";

export function createBootstrapQueries(api: ServerApi["bootstrap"]) {
    return {
        status: () =>
            queryOptions({
                queryKey: [baseKey, "status"],
                queryFn: () => api.status(),
            }),
    };
}

export type BootstrapQueries = ReturnType<typeof createBootstrapQueries>;
