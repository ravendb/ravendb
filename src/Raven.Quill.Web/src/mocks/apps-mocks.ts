import { delay, http, HttpResponse, ws, type RequestHandler } from "msw";
import type { AppResponse, ProvisionAgentResponse, SuggestAgentResponse } from "@/api/generated/server-api";
import type { AgentStreamEvent } from "@/api/custom-services/agent-stream";
import type { CdcLiveRawFrame } from "@/pages/apps/use-cdc-live-performance";
import { apiHttp } from "./api-http";

// WS-only relay route, so it has no generated client or `apiHttp` path to lean on.
const cdcProgressFeed = ws.link("ws://*/api/apps/:slug/cdc/progress");

export const appsMocks = {
    list: (apps: AppResponse[] = sampleApps) => apiHttp.get("/api/apps", ({ response }) => response(200).json(apps)),
    // msw-storybook-addon's typings only know RequestHandler, but its runtime forwards
    // every handler to worker.use, which accepts WebSocket handlers since msw 2.6.
    cdcProgress: (frame: CdcLiveRawFrame = sampleCdcProgressFrame()) =>
        cdcProgressFeed.addEventListener("connection", ({ client }) => {
            client.send(JSON.stringify(frame));
        }) as unknown as RequestHandler,
    detail: (apps: AppResponse[] = sampleApps) =>
        apiHttp.get("/api/apps/{slug}", ({ params, response }) => {
            const app = apps.find((candidate) => candidate.slug === params.slug);
            return app ? response(200).json(app) : response(404).json({ error: `Unknown app: ${params.slug}` });
        }),
    provisionAgent: (result: ProvisionAgentResponse = { agentId: "agents/sales" }) =>
        apiHttp.post("/api/apps/{slug}/setup/agent", ({ response }) => response(200).json(result)),
    // setup/try streams newline-delimited JSON events (not described by the OpenAPI
    // contract), so this mock uses plain msw instead of `apiHttp` — mirroring chatMocks.
    setupTry: (events: AgentStreamEvent[] = sampleAgentTestEvents, chunkDelayMs = 150) =>
        http.post("/api/apps/:slug/setup/try", () => {
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
    suggestAgent: (suggestion: SuggestAgentResponse = sampleAgentSuggestion) =>
        apiHttp.post("/api/apps/{slug}/suggest/agent", ({ response }) => response(200).json(suggestion)),
};

export const sampleApps: AppResponse[] = [
    {
        id: "apps/1",
        slug: "demo",
        name: "Demo Shop",
        database: "demo-shop",
        cdcTaskName: "cdc/demo-shop",
        createdAt: "2026-05-01T10:00:00Z",
    },
    {
        id: "apps/2",
        slug: "support",
        name: "Support Desk",
        database: "support-desk",
        cdcTaskName: "cdc/support-desk",
        createdAt: "2026-05-12T08:30:00Z",
    },
];

// Recent, now-relative batches so the live shaper reads them as an active feed.
export function sampleCdcProgressFrame(): CdcLiveRawFrame {
    return {
        Results: [
            {
                TaskName: "cdc/demo-shop",
                Stats: [
                    {
                        Performance: Array.from({ length: 6 }, (_, index) => {
                            const startedMs = Date.now() - 5_000 - index * 90_000;
                            const durationInMs = 900 + Math.round(Math.abs(Math.sin(index)) * 800);
                            const read = 480 + index * 7;
                            return {
                                Id: 6 - index,
                                Started: new Date(startedMs).toISOString(),
                                Completed: new Date(startedMs + durationInMs).toISOString(),
                                DurationInMs: durationInMs,
                                NumberOfReadMessages: read,
                                NumberOfProcessedMessages: read,
                                ScriptProcessingErrorCount: 0,
                                ReadErrorCount: 0,
                            };
                        }),
                    },
                ],
            },
        ],
    };
}

export const sampleAgentSuggestion: SuggestAgentResponse = {
    configurations: [
        {
            identifier: "sales-assistant",
            name: "Sales assistant",
            connectionStringName: "openai-chat",
            systemPrompt: "You help customers of the Demo Shop find products and check their orders.",
            sampleObject: JSON.stringify(
                {
                    reply: "A helpful answer to the customer's question.",
                    relatedProducts: "Up to three product names worth suggesting.",
                },
                null,
                4,
            ),
            queries: [
                {
                    name: "search-products",
                    description: "Search products by name.",
                    query: "from Products where search(Name, $searchTerm)",
                },
            ],
            parameters: [{ name: "customerId", description: "Id of the signed-in customer." }],
        },
    ],
    rationale: ["The database contains products and orders, so a shopping assistant fits well."],
    status: "Success",
};

export const sampleAgentTestEvents: AgentStreamEvent[] = [
    { type: "chunk", text: "Sure! " },
    { type: "chunk", text: "Based on the demo data, " },
    { type: "chunk", text: "I can help you find products and check orders." },
    {
        type: "done",
        answer: { reply: "Sure! Based on the demo data, I can help you find products and check orders." },
        // The full structured model output the wizard renders as JSON (server's `fullAnswer`).
        fullAnswer: {
            reply: "Sure! Based on the demo data, I can help you find products and check orders.",
            relatedProducts: ["Wireless Mouse", "USB-C Hub", "Laptop Stand"],
        },
        // The query tools the agent ran this turn (server's `toolCalls`): the RQL, the parameters
        // the model filled in, and the rows the query returned.
        toolCalls: [
            {
                id: "call_1",
                name: "search-products",
                description: "Search products by name.",
                query: "from Products where search(Name, $searchTerm)",
                arguments: JSON.stringify({ searchTerm: "mouse" }),
                result: JSON.stringify([
                    { Name: "Wireless Mouse", Price: 24.99 },
                    { Name: "USB-C Hub", Price: 39.0 },
                    { Name: "Laptop Stand", Price: 29.5 },
                ]),
            },
        ],
        conversationId: "chats/test",
    },
];
