import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ChatRequest } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";
import { streamAgentNdjson, type AgentStreamEvent } from "@/api/custom-services/agent-stream";

// The chat/embed streams never send the wizard-only `fullAnswer`/`toolCalls` fields, so narrow
// the shared agent stream to the frames chat actually receives (its `done` carries `answer` only).
export type ChatStreamEvent =
    | Extract<AgentStreamEvent, { type: "chunk" | "error" }>
    | Omit<Extract<AgentStreamEvent, { type: "done" }>, "fullAnswer" | "toolCalls">;

export function createChatService(client: ApiClient) {
    return {
        stream: (request: ChatRequest, signal?: AbortSignal): AsyncGenerator<ChatStreamEvent> =>
            streamAgentNdjson(client, API_ENDPOINTS.chat.stream, request, signal),
    };
}

export type ChatService = ReturnType<typeof createChatService>;
