import { createAuthQueries, createAuthService } from "@/api/auth-service";
import { createAppsService, createAppsQueries } from "@/api/apps-service";
import { createApiClient, type ApiClient, type ApiClientOptions } from "@/api/http-client";

export type ApiServices = {
    auth: ReturnType<typeof createAuthService>;
    apps: ReturnType<typeof createAppsService>;
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
        auth: createAuthService(client),
        apps: createAppsService(client),
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
