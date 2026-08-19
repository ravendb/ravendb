import { createApiClient, type ApiClient, type ApiClientOptions } from "@/api/http-client";
import { createServerApi, type ServerApi } from "@/api/generated/server-api";
import { createAgentTestService } from "@/api/custom-services/agent-test-service";
import { createAssistantService } from "@/api/custom-services/assistant-service";
import { createCertificatesService } from "@/api/custom-services/certificates-service";
import { createSetupSuggestionsService } from "@/api/custom-services/setup-suggestions-service";
import { createCertificatesQueries } from "@/api/queries/certificates-queries";
import { createAppsQueries } from "@/api/queries/apps-queries";
import { createAgentsQueries } from "@/api/queries/agents-queries";
import { createAssistantQueries } from "@/api/queries/assistant-queries";
import { createChannelsQueries } from "@/api/queries/channels-queries";
import { createWebWidgetQueries } from "@/api/queries/web-widget-queries";
import { createEmbedLinksQueries } from "@/api/queries/embed-links-queries";
import { createBootstrapQueries } from "@/api/queries/bootstrap-queries";
import { createSetupQueries } from "@/api/queries/setup-queries";
import { createAiConnectionStringsQueries } from "@/api/queries/ai-connection-strings-queries";
import { createAiModelsQueries } from "@/api/queries/ai-models-queries";
import { createStatsQueries } from "@/api/queries/stats-queries";
import { createSettingsQueries } from "@/api/queries/settings-queries";
import { createSlackQueries } from "@/api/queries/slack-queries";

export type ApiServices = ServerApi & {
    agentTest: ReturnType<typeof createAgentTestService>;
    assistantChat: ReturnType<typeof createAssistantService>;
    certificates: ReturnType<typeof createCertificatesService>;
    setupSuggestions: ReturnType<typeof createSetupSuggestionsService>;
};

export type ApiQueries = {
    bootstrap: ReturnType<typeof createBootstrapQueries>;
    apps: ReturnType<typeof createAppsQueries>;
    agents: ReturnType<typeof createAgentsQueries>;
    assistant: ReturnType<typeof createAssistantQueries>;
    channels: ReturnType<typeof createChannelsQueries>;
    slack: ReturnType<typeof createSlackQueries>;
    webWidget: ReturnType<typeof createWebWidgetQueries>;
    embedLinks: ReturnType<typeof createEmbedLinksQueries>;
    setup: ReturnType<typeof createSetupQueries>;
    aiConnectionStrings: ReturnType<typeof createAiConnectionStringsQueries>;
    aiModels: ReturnType<typeof createAiModelsQueries>;
    stats: ReturnType<typeof createStatsQueries>;
    settings: ReturnType<typeof createSettingsQueries>;
    certificates: ReturnType<typeof createCertificatesQueries>;
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
        agentTest: createAgentTestService(client),
        assistantChat: createAssistantService(client),
        certificates: createCertificatesService(client),
        setupSuggestions: createSetupSuggestionsService(client),
    };

    return {
        client,
        services,
        queries: {
            bootstrap: createBootstrapQueries(services.bootstrap),
            apps: createAppsQueries(services.apps),
            agents: createAgentsQueries(services.agents),
            assistant: createAssistantQueries(services.assistant),
            channels: createChannelsQueries(services.channels),
            slack: createSlackQueries(services.slack),
            webWidget: createWebWidgetQueries(services.iframe),
            embedLinks: createEmbedLinksQueries(services.embedLinks),
            setup: createSetupQueries(services.setup),
            aiConnectionStrings: createAiConnectionStringsQueries(services.aiConnectionStrings),
            aiModels: createAiModelsQueries(services.aiModels),
            stats: createStatsQueries(services.stats),
            settings: createSettingsQueries(services.settings),
            certificates: createCertificatesQueries(services.certificates),
        },
    };
}

export const api = createApi();
