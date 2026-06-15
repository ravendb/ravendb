import type { AppResponse, ProvisionAgentResponse, SuggestAgentResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const appsMocks = {
    list: (apps: AppResponse[] = sampleApps) => apiHttp.get("/api/apps", ({ response }) => response(200).json(apps)),
    detail: (apps: AppResponse[] = sampleApps) =>
        apiHttp.get("/api/apps/{slug}", ({ params, response }) => {
            const app = apps.find((candidate) => candidate.slug === params.slug);
            return app ? response(200).json(app) : response(404).json({ error: `Unknown app: ${params.slug}` });
        }),
    provisionAgent: (result: ProvisionAgentResponse = { agentId: "agents/sales" }) =>
        apiHttp.post("/api/apps/{slug}/setup/agent", ({ response }) => response(200).json(result)),
    setupTry: () => apiHttp.post("/api/apps/{slug}/setup/try", ({ response }) => response(200).empty()),
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

export const sampleAgentSuggestion: SuggestAgentResponse = {
    configurations: [
        {
            identifier: "sales-assistant",
            name: "Sales assistant",
            connectionStringName: "openai-chat",
            systemPrompt: "You help customers of the Demo Shop find products and check their orders.",
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
