import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/http-client";

export type ApplianceApp = {
    id: string;
    name: string;
    createdAt: string;
};

export type CreateAppRequest = {
    name: string;
};

const mockedApp: ApplianceApp = {
    id: "demo-app",
    name: "Demo App",
    createdAt: new Date().toISOString(),
};

const mockedApps: ApplianceApp[] = [mockedApp];

export function createAppsService(client: ApiClient) {
    return {
        create: (request: CreateAppRequest) => client.post<ApplianceApp>("/apps", request),
        get: (appId: string) => {
            const app = mockedApps.find((item) => item.id === appId);
            return Promise.resolve<ApplianceApp>(app ?? { ...mockedApp, id: appId });

            // TODO uncomment when API is ready
            // return client.get<ApplianceApp>(`/apps/${appId}`)
        },
        list: () => {
            return Promise.resolve<ApplianceApp[]>(mockedApps);

            // TODO uncomment when API is ready
            // return client.get<ApplianceApp[]>("/apps");
        },
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
