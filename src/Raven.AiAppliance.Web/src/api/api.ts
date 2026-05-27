import { createApiClient, type ApiClient, type ApiClientOptions } from "@/api/http-client";
import { createServerApi, type ServerApi } from "@/api/generated/server-api";
import { createChatService } from "@/api/custom-services/chat-service";
import { createAppsQueries } from "@/api/queries/apps-queries";
import { createBootstrapQueries } from "@/api/queries/bootstrap-queries";

export type ApiServices = Omit<ServerApi, "chat"> & {
    chat: ReturnType<typeof createChatService>;
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
    const generatedServices = createServerApi(client);
    const services = {
        ...generatedServices,
        chat: createChatService(client),
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
