import type { AgentSummaryResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const agentsMocks = {
    list: (agents: AgentSummaryResponse[] = sampleAgents) =>
        apiHttp.get("/api/apps/{slug}/agents", ({ response }) => response(200).json(agents)),
};

export const sampleAgents: AgentSummaryResponse[] = [
    {
        agentId: "agents/sales",
        name: "Sales assistant",
        model: "claude-opus-4-8",
        disabled: false,
        parameters: ["customerId"],
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
