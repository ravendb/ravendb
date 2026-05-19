import { createAuthApi, createAuthQueries } from "@/api/authApi";
import { createAppsApi, createAppsQueries } from "@/api/appsApi";
import {
  createApiClient,
  type ApiClient,
  type ApiClientOptions,
} from "@/api/httpClient";

export type ApiServices = {
  auth: ReturnType<typeof createAuthApi>;
  apps: ReturnType<typeof createAppsApi>;
};

export type ApiQueries = {
  auth: ReturnType<typeof createAuthQueries>;
  apps: ReturnType<typeof createAppsQueries>;
};

export type Api = {
  client: ApiClient;
  queries: ApiQueries;
  services: ApiServices;
};

export function createApi(options?: ApiClientOptions): Api {
  const client = createApiClient(options);
  const services = {
    auth: createAuthApi(client),
    apps: createAppsApi(client),
  };

  return {
    client,
    services,
    queries: {
      auth: createAuthQueries(services.auth),
      apps: createAppsQueries(services.apps),
    },
  };
}

export const api = createApi();
