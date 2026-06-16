import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { SetupTryRequest } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";
import { streamAgentNdjson, type AgentStreamEvent } from "@/api/custom-services/agent-stream";

// Streams a single "Test agent" turn against the wizard's draft configuration via
// /api/apps/{slug}/setup/try. The generated client types this endpoint as `void` (it streams
// NDJSON rather than a JSON body), so the wizard uses this custom streaming service instead.
export function createAgentTestService(client: ApiClient) {
    return {
        stream: (slug: string, request: SetupTryRequest): AsyncGenerator<AgentStreamEvent> =>
            streamAgentNdjson(client, API_ENDPOINTS.apps.setupTry(slug), request),
    };
}

export type AgentTestService = ReturnType<typeof createAgentTestService>;
