import { z } from "zod";
import type { ApiClient } from "@/api/http-client";

// Shared NDJSON streaming for the agent endpoints (chat + wizard "Test agent"). Both stream
// the same frame shape: incremental `chunk`s, a terminal `done` (with the full answer), or an
// `error`.
const agentStreamEventSchema = z.discriminatedUnion("type", [
    z.object({
        type: z.literal("chunk"),
        text: z.string(),
    }),
    z.object({
        type: z.literal("done"),
        answer: z.unknown(),
        // The full structured model output (every declared field). Present on the wizard's
        // "Test agent" stream; absent on the chat/embed streams, which only send `answer`.
        fullAnswer: z.unknown().optional(),
        conversationId: z.string(),
    }),
    z.object({
        type: z.literal("error"),
        message: z.string(),
    }),
]);

export type AgentStreamEvent = z.infer<typeof agentStreamEventSchema>;

/** POSTs `body` to `path` and yields the NDJSON frames the server streams back. Non-2xx
 * responses (e.g. a pre-stream 400/404) throw an ApiError before the first frame. */
export async function* streamAgentNdjson(
    client: ApiClient,
    path: string,
    body: unknown,
): AsyncGenerator<AgentStreamEvent> {
    const response = await client.post<Response>(path, body, { responseType: "response" });

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
                    yield parseAgentStreamEvent(line);
                }
            }
        }

        if (buffer.trim()) {
            yield parseAgentStreamEvent(buffer);
        }
    } finally {
        reader.releaseLock();
    }
}

function parseAgentStreamEvent(line: string): AgentStreamEvent {
    const result = agentStreamEventSchema.safeParse(JSON.parse(line) as unknown);

    if (!result.success) {
        throw new Error("Received an invalid agent stream event.");
    }

    return result.data;
}
