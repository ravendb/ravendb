import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/http-client";

export type ApplianceApp = {
    id: string;
    name: string;
    database: string;
    cdcTaskName: string;
    createdAt: string;
};

export function createAppsService(client: ApiClient) {
    return {
        get: (appId: string) => client.get<ApplianceApp>(`/apps/${appId}`),
        list: () => client.get<ApplianceApp[]>("/apps"),
    };
}

export type AppsService = ReturnType<typeof createAppsService>;

export function createAppsQueries(api: AppsService) {
    return {
        detail: (appId: string) =>
            queryOptions({
                queryKey: ["apps", "detail", appId],
                queryFn: () => api.get(appId),
            }),
        list: () =>
            queryOptions({
                queryKey: ["apps", "list"],
                queryFn: () => api.list(),
            }),
    };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
