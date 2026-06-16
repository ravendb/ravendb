import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ChatRequest } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";
import { streamAgentNdjson, type AgentStreamEvent } from "@/api/custom-services/agent-stream";

export type ChatStreamEvent = AgentStreamEvent;

export function createChatService(client: ApiClient) {
    return {
        stream: (request: ChatRequest): AsyncGenerator<ChatStreamEvent> =>
            streamAgentNdjson(client, API_ENDPOINTS.chat.stream, request),
    };
}

export type ChatService = ReturnType<typeof createChatService>;
