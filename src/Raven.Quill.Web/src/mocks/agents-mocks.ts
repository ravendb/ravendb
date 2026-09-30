import type {
    AgentDetailsResponse,
    AgentSummaryResponse,
    AiAgentConfiguration,
    ProvisionAgentResponse,
    WebhookBinding,
} from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const agentsMocks = {
    list: (agents: AgentSummaryResponse[] = sampleAgents) =>
        apiHttp.get("/api/apps/{slug}/agents", ({ response }) => response(200).json(agents)),
    get: (details: AgentDetailsResponse = sampleAgentDetails) =>
        apiHttp.get("/api/apps/{slug}/agent/{agentId}", ({ response }) => response(200).json(details)),
    edit: (result: ProvisionAgentResponse = { agentId: "agents/sales" }) =>
        apiHttp.post("/api/apps/{slug}/agent", ({ response }) => response(200).json(result)),
    delete: () => apiHttp.delete("/api/apps/{slug}/agent/{agentId}", ({ response }) => response(204).empty()),
};

export const sampleAgents: AgentSummaryResponse[] = [
    {
        agentId: "agents/sales",
        name: "Sales assistant",
        model: "claude-opus-4-8",
        disabled: false,
        parameters: [
            { name: "customerId", description: "The customer to scope queries to.", type: "String" },
            { name: "orderLimit", description: "How many orders to consider.", type: "Number" },
            { name: "includeDrafts", description: "Whether draft orders count.", type: "Boolean" },
            { name: "regions", description: "Regions to search.", type: "ArrayOfString" },
        ],
        lastInvokedAt: "2026-06-25T09:00:00Z",
        conversations: 1800,
        messages: 5200,
        tokens: 1200000,
    },
    {
        agentId: "agents/faq",
        name: "FAQ bot",
        model: "claude-haiku-4-5",
        disabled: true,
        parameters: [],
        lastInvokedAt: null,
        conversations: 0,
        messages: 0,
        tokens: 0,
    },
];

export const sampleAgentConfiguration: AiAgentConfiguration = {
    identifier: "agents/sales",
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
    outputSchema: null,
    queries: [
        {
            name: "search-products",
            description: "Search products by name.",
            query: "from Products where search(Name, $searchTerm)",
            parametersSampleObject: "{}",
        },
    ],
    parameters: [
        {
            name: "customerId",
            description: "Id of the signed-in customer.",
            type: "String",
            policy: "Default",
            sendToModel: true,
        },
    ],
    actions: [
        {
            name: "create_ticket",
            description: "Open a support ticket for the customer.",
            parametersSampleObject: JSON.stringify({ subject: "What the customer needs help with." }, null, 4),
        },
    ],
    subAgents: [],
    disabled: false,
};

export const sampleActionBindings: Record<string, WebhookBinding> = {
    create_ticket: { url: "https://hooks.demo-shop.example/tickets", secret: "s3cret", maxResponseSize: 2048 },
};

export const sampleAgentDetails: AgentDetailsResponse = {
    configuration: sampleAgentConfiguration,
    actionBindings: sampleActionBindings,
};
