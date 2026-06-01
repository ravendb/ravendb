import { z } from "zod";
import { API_ENDPOINTS } from "@/api/generated/server-api";
import type { ChatRequest } from "@/api/generated/server-api";
import type { ApiClient } from "@/api/http-client";

const chatStreamEventSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("chunk"),
        text: z.string(),
    }),
    z.object({
        type: z.literal("done"),
        answer: z.unknown(),
        conversationId: z.string(),
    }),
    z.object({
        type: z.literal("error"),
        message: z.string(),
    }),
]);

export type ChatStreamEvent = z.infer<typeof chatStreamEventSchema>;

export function createChatService(client: ApiClient) {
    return {
        stream: async function* (request: ChatRequest): AsyncGenerator<ChatStreamEvent> {
            const response = await client.post<Response>(API_ENDPOINTS.chat.stream, request, {
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
                            yield parseChatStreamEvent(line);
                        }
                    }
                }

                if (buffer.trim()) {
                    yield parseChatStreamEvent(buffer);
                }
            } finally {
                reader.releaseLock();
            }
        },
    };
}

export type ChatService = ReturnType<typeof createChatService>;

function parseChatStreamEvent(line: string) {
    const result = chatStreamEventSchema.safeParse(JSON.parse(line) as unknown);

    if (!result.success) {
        throw new Error("Received an invalid chat stream event.");
    }

    return result.data;
}
