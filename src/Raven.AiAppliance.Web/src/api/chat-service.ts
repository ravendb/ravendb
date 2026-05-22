import type { ApiClient } from "@/api/http-client";

export type ChatRequest = {
    agentId: string;
    prompt: string;
    conversationId?: string | null;
    parameters?: Record<string, string> | null;
};

export type ChatStreamEvent =
    | {
          type: "chunk";
          text: string;
      }
    | {
          type: "done";
          answer: unknown;
          conversationId: string;
      }
    | {
          type: "error";
          message: string;
      };

export function createChatService(client: ApiClient) {
    return {
        stream: async function* (request: ChatRequest): AsyncGenerator<ChatStreamEvent> {
            const response = await client.post<Response>("/chat/stream", request, {
                responseType: "response",
            });

            if (!response.body) {
                return;
            }

            const reader = response.body.pipeThrough(new TextDecoderStream()).getReader();
            let buffer = "";

            try {
                while (true) {
                    const { done, value } = await reader.read();
                    if (done) {
                        break;
                    }

                    buffer += value;
                    const lines = buffer.split("\n");
                    buffer = lines.pop() ?? "";

                    for (const line of lines) {
                        if (line.trim()) {
                            yield JSON.parse(line) as ChatStreamEvent;
                        }
                    }
                }

                if (buffer.trim()) {
                    yield JSON.parse(buffer) as ChatStreamEvent;
                }
            } finally {
                reader.releaseLock();
            }
        },
    };
}

export type ChatService = ReturnType<typeof createChatService>;
