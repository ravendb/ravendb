import { createApiClient, type ApiClient, type ApiClientOptions } from "@/api/http-client";
import { createServerApi, type ServerApi } from "@/api/generated/server-api";
import { createChatService } from "@/api/custom-services/chat-service";
import { createAgentTestService } from "@/api/custom-services/agent-test-service";
import { createAppsQueries } from "@/api/queries/apps-queries";
import { createAgentsQueries } from "@/api/queries/agents-queries";
import { createChannelsQueries } from "@/api/queries/channels-queries";
import { createEmbedLinksQueries } from "@/api/queries/embed-links-queries";
import { createBootstrapQueries } from "@/api/queries/bootstrap-queries";
import { createSetupQueries } from "@/api/queries/setup-queries";
import { createAiConnectionStringsQueries } from "@/api/queries/ai-connection-strings-queries";
import { createStatsQueries } from "@/api/queries/stats-queries";
import { createSettingsQueries } from "@/api/queries/settings-queries";

export type ApiServices = Omit<ServerApi, "chat"> & {
    chat: ReturnType<typeof createChatService>;
    agentTest: ReturnType<typeof createAgentTestService>;
};

export type ApiQueries = {
    bootstrap: ReturnType<typeof createBootstrapQueries>;
    apps: ReturnType<typeof createAppsQueries>;
    agents: ReturnType<typeof createAgentsQueries>;
    channels: ReturnType<typeof createChannelsQueries>;
    embedLinks: ReturnType<typeof createEmbedLinksQueries>;
    setup: ReturnType<typeof createSetupQueries>;
    aiConnectionStrings: ReturnType<typeof createAiConnectionStringsQueries>;
    stats: ReturnType<typeof createStatsQueries>;
    settings: ReturnType<typeof createSettingsQueries>;
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
        agentTest: createAgentTestService(client),
    };

    return {
        client,
        services,
        queries: {
            bootstrap: createBootstrapQueries(services.bootstrap),
            apps: createAppsQueries(services.apps),
            agents: createAgentsQueries(services.agents),
            channels: createChannelsQueries(services.channels),
            embedLinks: createEmbedLinksQueries(services.embedLinks),
            setup: createSetupQueries(services.setup),
            aiConnectionStrings: createAiConnectionStringsQueries(services.aiConnectionStrings),
            stats: createStatsQueries(services.stats),
            settings: createSettingsQueries(services.settings),
        },
    };
}

export const api = createApi();
