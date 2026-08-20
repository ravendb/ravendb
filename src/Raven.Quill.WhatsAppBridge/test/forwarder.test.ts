import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { pino } from "pino";
import { InboundForwarder, type InboundPayload } from "../src/inbound-forwarder.js";

const logger = pino({ level: "silent" });

const payload: InboundPayload = {
    database: "db",
    channelId: "c".repeat(32),
    sender: "48123456789@s.whatsapp.net",
    messageId: "MSG1",
    kind: "text",
    text: "hello",
    timestamp: 1754300000,
};

function fakeFetch(outcomes: Array<number | Error>): { impl: typeof fetch; calls: () => number } {
    let call = 0;
    const impl = (async () => {
        const outcome = outcomes[Math.min(call++, outcomes.length - 1)];
        if (outcome instanceof Error)
            throw outcome;
        return new Response(null, { status: outcome });
    }) as typeof fetch;
    return { impl, calls: () => call };
}

describe("InboundForwarder", () => {
    it("delivers on first success", async () => {
        const fetch = fakeFetch([202]);
        await new InboundForwarder("http://web", "t", logger, [1, 1, 1], fetch.impl).forward(payload);
        assert.equal(fetch.calls(), 1);
    });

    it("retries network errors then succeeds", async () => {
        const fetch = fakeFetch([new Error("refused"), new Error("refused"), 202]);
        await new InboundForwarder("http://web", "t", logger, [1, 1, 1], fetch.impl).forward(payload);
        assert.equal(fetch.calls(), 3);
    });

    it("retries non-2xx responses", async () => {
        const fetch = fakeFetch([503, 202]);
        await new InboundForwarder("http://web", "t", logger, [1, 1, 1], fetch.impl).forward(payload);
        assert.equal(fetch.calls(), 2);
    });

    it("drops after the retry budget without throwing", async () => {
        const fetch = fakeFetch([new Error("refused")]);
        await new InboundForwarder("http://web", "t", logger, [1, 1, 1], fetch.impl).forward(payload);
        assert.equal(fetch.calls(), 4);
    });
});
