import { delay, http, HttpResponse } from "msw";
import type { ChatStreamEvent } from "@/api/custom-services/chat-service";

export const chatMocks = {
    // The chat endpoint streams newline-delimited JSON events, which the OpenAPI
    // contract does not describe, so this mock uses plain msw instead of `apiHttp`.
    stream: (events: ChatStreamEvent[] = sampleChatEvents, chunkDelayMs = 200) =>
        http.post("/api/chat/stream", () => {
            const encoder = new TextEncoder();
            const stream = new ReadableStream<Uint8Array>({
                async start(controller) {
                    for (const event of events) {
                        await delay(chunkDelayMs);
                        controller.enqueue(encoder.encode(`${JSON.stringify(event)}\n`));
                    }

                    controller.close();
                },
            });

            return new HttpResponse(stream, { headers: { "Content-Type": "application/x-ndjson" } });
        }),
};

export const sampleChatEvents: ChatStreamEvent[] = [
    { type: "chunk", text: "Hi! " },
    { type: "chunk", text: "I am the mocked sales assistant. " },
    { type: "chunk", text: "How can I help you today?" },
    {
        type: "done",
        answer: "Hi! I am the mocked sales assistant. How can I help you today?",
        conversationId: "conversations/1",
    },
];
