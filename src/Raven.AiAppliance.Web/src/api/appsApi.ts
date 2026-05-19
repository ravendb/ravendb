import { queryOptions } from "@tanstack/react-query";
import type { ApiClient } from "@/api/httpClient";

export type ApplianceApp = {
  id: string;
  name: string;
  createdAt: string;
};

export type CreateAppRequest = {
  name: string;
};

export function createAppsApi(client: ApiClient) {
  return {
    create: (request: CreateAppRequest) =>
      client.post<ApplianceApp>("/apps", request),
    get: (appId: string) => client.get<ApplianceApp>(`/apps/${appId}`),
    list: () => client.get<ApplianceApp[]>("/apps"),
  };
}

export type AppsApi = ReturnType<typeof createAppsApi>;

const appsKeys = {
  all: ["apps"],
  detail: (appId: string) => [...appsKeys.all, "detail", appId],
  lists: () => [...appsKeys.all, "list"],
} as const;

export function createAppsQueries(api: AppsApi) {
  return {
    detail: (appId: string) =>
      queryOptions({
        queryKey: appsKeys.detail(appId),
        queryFn: () => api.get(appId),
      }),
    list: () =>
      queryOptions({
        queryKey: appsKeys.lists(),
        queryFn: () => api.list(),
      }),
  };
}

export type AppsQueries = ReturnType<typeof createAppsQueries>;
