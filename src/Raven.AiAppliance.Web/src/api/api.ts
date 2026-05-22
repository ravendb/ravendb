import { createBootstrapQueries, createBootstrapService } from "@/api/bootstrap-service";
import { createChatService } from "@/api/chat-service";
import { createAppsService, createAppsQueries } from "@/api/apps-service";
import { createApiClient, type ApiClient, type ApiClientOptions } from "@/api/http-client";
import { createSetupService } from "@/api/setup-service";

export type ApiServices = {
    bootstrap: ReturnType<typeof createBootstrapService>;
    apps: ReturnType<typeof createAppsService>;
    chat: ReturnType<typeof createChatService>;
    setup: ReturnType<typeof createSetupService>;
};

export type ApiQueries = {
    bootstrap: ReturnType<typeof createBootstrapQueries>;
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
        bootstrap: createBootstrapService(client),
        apps: createAppsService(client),
        chat: createChatService(client),
        setup: createSetupService(client),
    };

    return {
        client,
        services,
        queries: {
            bootstrap: createBootstrapQueries(services.bootstrap),
            apps: createAppsQueries(services.apps),
        },
    };
}

export const api = createApi();
