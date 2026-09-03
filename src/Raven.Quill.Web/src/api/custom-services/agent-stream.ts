import { z } from "zod";
import type { ApiClient } from "@/api/http-client";
import { streamNdjsonLines } from "@/api/custom-services/response-stream";

// One query tool the agent invoked during the turn, reconstructed server-side from the
// conversation transcript: the configured RQL + description, the parameters the model filled
// in, and the content the query returned. Present on the wizard's "Test agent" stream (which
// supports query tools only); the chat/embed streams don't send tool calls.
const agentToolCallSchema = z.object({
    id: z.string(),
    name: z.string(),
    description: z.string().nullish(),
    query: z.string().nullish(),
    arguments: z.string().nullish(),
    result: z.string().nullish(),
});

export type AgentToolCall = z.infer<typeof agentToolCallSchema>;

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
        // The query tools the agent ran this turn. Present (possibly empty) on the wizard's
        // "Test agent" stream; absent on the chat/embed streams.
        toolCalls: z.array(agentToolCallSchema).optional(),
        conversationId: z.string(),
    }),
    z.object({
        type: z.literal("error"),
        message: z.string(),
    }),
]);

export type AgentStreamEvent = z.infer<typeof agentStreamEventSchema>;

/** POSTs `body` to `path` and yields the NDJSON frames the server streams back. Non-2xx
 * responses (e.g. a pre-stream 400/404) throw an ApiError before the first frame. Pass a
 * `signal` to cancel the request (the caller aborts it when the panel unmounts mid-stream). */
export async function* streamAgentNdjson(
    client: ApiClient,
    path: string,
    body: unknown,
    signal?: AbortSignal,
): AsyncGenerator<AgentStreamEvent> {
    for await (const line of streamNdjsonLines(client, path, body, signal)) {
        yield parseAgentStreamEvent(line);
    }
}

function parseAgentStreamEvent(line: string): AgentStreamEvent {
    const result = agentStreamEventSchema.safeParse(JSON.parse(line) as unknown);

    if (!result.success) {
        throw new Error("Received an invalid agent stream event.");
    }

    return result.data;
}
