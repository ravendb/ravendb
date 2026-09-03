import { describe, expect, it } from "vitest";
import { streamNdjsonLines, streamSseData } from "@/api/custom-services/response-stream";
import { createApiClient, type ApiTransport } from "@/api/http-client";

// Each argument is one read the browser hands back, so a test can put a frame boundary anywhere.
function clientStreaming(...reads: string[]) {
    const transport: ApiTransport = () => {
        const encoder = new TextEncoder();
        const body = new ReadableStream<Uint8Array>({
            start(controller) {
                for (const read of reads) {
                    controller.enqueue(encoder.encode(read));
                }

                controller.close();
            },
        });

        return Promise.resolve(new Response(body, { headers: { "Content-Type": "text/event-stream" } }));
    };

    return createApiClient({ transport });
}

async function collect(lines: AsyncGenerator<string>) {
    const collected: string[] = [];

    for await (const line of lines) {
        collected.push(line);
    }

    return collected;
}

describe("streamSseData", () => {
    it("joins a frame split across two reads", async () => {
        const client = clientStreaming('data: {"type":"On', 'going","text":"hi"}\n\n');

        await expect(collect(streamSseData(client, "/assistant/chat", {}))).resolves.toEqual([
            '{"type":"Ongoing","text":"hi"}',
        ]);
    });

    it("skips comments, other fields and the blank lines between events", async () => {
        const client = clientStreaming(":keep-alive\r\n", "event: message\r\n", 'data:{"a":1}\r\n', "\r\n");

        await expect(collect(streamSseData(client, "/assistant/chat", {}))).resolves.toEqual(['{"a":1}']);
    });

    it("discards a trailing line the connection cut off before its newline", async () => {
        const client = clientStreaming('data: {"a":1}\n\n', 'data: {"b":');

        await expect(collect(streamSseData(client, "/assistant/chat", {}))).resolves.toEqual(['{"a":1}']);
    });

    it("skips a data field with no payload", async () => {
        const client = clientStreaming("data:\n\n", 'data: {"a":1}\n\n');

        await expect(collect(streamSseData(client, "/assistant/chat", {}))).resolves.toEqual(['{"a":1}']);
    });

    it("throws before the first frame when the request itself failed", async () => {
        const client = createApiClient({
            transport: () =>
                Promise.resolve(
                    new Response(JSON.stringify({ error: "message is required" }), {
                        status: 400,
                        headers: { "Content-Type": "application/json" },
                    }),
                ),
        });

        await expect(collect(streamSseData(client, "/assistant/chat", {}))).rejects.toThrow("message is required");
    });
});

describe("streamNdjsonLines", () => {
    it("holds a partial line until the read that completes it and drops blank ones", async () => {
        const client = clientStreaming('{"a":1}\n{"b":', '2}\n\n{"c":3}');

        await expect(collect(streamNdjsonLines(client, "/chat/stream", {}))).resolves.toEqual([
            '{"a":1}',
            '{"b":2}',
            '{"c":3}',
        ]);
    });
});
