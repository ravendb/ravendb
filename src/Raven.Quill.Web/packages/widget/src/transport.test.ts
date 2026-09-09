import { afterEach, describe, expect, it, vi } from "vitest";
import { streamChat } from "@/transport";

async function eventsFor(response: Response) {
    vi.stubGlobal("fetch", vi.fn().mockResolvedValue(response));

    const events = [];
    for await (const event of streamChat("https://example.test/chat", "hi", new AbortController().signal))
        events.push(event);
    return events;
}

afterEach(() => {
    vi.unstubAllGlobals();
});

// The server answers 429 from two places, and only one of them may brick the widget: the link's own
// invocation budget (a JSON body carrying `code: "invocation_limit"`) is terminal, while the per-IP
// throttle (an empty body) must leave the composer usable for a retry.
describe("streamChat on 429", () => {
    it("treats an invocation-limit body as the terminal limit", async () => {
        const body = JSON.stringify({ error: "this link has reached its usage limit", code: "invocation_limit" });
        const events = await eventsFor(new Response(body, { status: 429 }));

        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({ type: "error", kind: "limit" });
    });

    it("keeps a bodiless 429 - the per-IP throttle - retryable", async () => {
        const events = await eventsFor(new Response(null, { status: 429 }));

        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({ type: "error", kind: "failed" });
    });

    it("keeps a 429 with an unrelated JSON body retryable", async () => {
        const events = await eventsFor(new Response(JSON.stringify({ error: "slow down" }), { status: 429 }));

        expect(events).toHaveLength(1);
        expect(events[0]).toMatchObject({ type: "error", kind: "failed" });
    });
});

describe("streamChat on a dead link", () => {
    it("maps 404 and 410 to the terminal expired state", async () => {
        for (const status of [404, 410]) {
            const events = await eventsFor(new Response(null, { status }));
            expect(events).toHaveLength(1);
            expect(events[0]).toMatchObject({ type: "error", kind: "expired" });
        }
    });
});

describe("streamChat on a malformed frame", () => {
    it("skips the bad line and finishes the turn", async () => {
        const body = [
            JSON.stringify({ type: "chunk", text: "one" }),
            "{not json",
            JSON.stringify({ type: "chunk", text: "two" }),
            JSON.stringify({ type: "done", answer: { reply: null } }),
        ].join("\n");
        const events = await eventsFor(new Response(`${body}\n`, { status: 200 }));

        expect(events).toEqual([
            { type: "chunk", text: "one" },
            { type: "chunk", text: "two" },
            { type: "done", reply: null },
        ]);
    });
});
