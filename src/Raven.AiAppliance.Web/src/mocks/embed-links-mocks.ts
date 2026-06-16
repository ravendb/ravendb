import type { ApiErrorResponse, EmbedLinkSummaryResponse, MintEmbedLinkResponse } from "@/api/generated/server-api";
import { SAMPLE_WEB_WIDGET_ID } from "./channels-mocks";
import { apiHttp } from "./api-http";

export const embedLinksMocks = {
    list: (links: EmbedLinkSummaryResponse[] = sampleEmbedLinks) =>
        apiHttp.get("/api/apps/{slug}/embed-links", ({ response }) => response(200).json(links)),
    mint: (result: MintEmbedLinkResponse = sampleMintedLink) =>
        apiHttp.post("/api/apps/{slug}/embed-links", ({ response }) => response(200).json(result)),
    mintError: (
        error: ApiErrorResponse = { error: "The “customerId” parameter is required.", code: "missing_parameters" },
    ) => apiHttp.post("/api/apps/{slug}/embed-links", ({ response }) => response(400).json(error)),
    revoke: () => apiHttp.delete("/api/apps/{slug}/embed-links/{token}", ({ response }) => response(204).empty()),
};

export const sampleEmbedLinks: EmbedLinkSummaryResponse[] = [
    {
        token: "3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
        widgetId: SAMPLE_WEB_WIDGET_ID,
        agentId: "agents/sales",
        parameters: { customerId: "users/1" },
        createdAt: "2026-06-14T09:00:00Z",
        expiresAt: "2026-06-20T09:00:00Z",
        maxInvocations: 100,
        invocationCount: 12,
    },
    {
        token: "9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d",
        widgetId: SAMPLE_WEB_WIDGET_ID,
        agentId: "agents/sales",
        parameters: { customerId: "users/42" },
        createdAt: "2026-06-15T11:30:00Z",
        expiresAt: "2026-06-16T11:30:00Z",
        maxInvocations: 50,
        invocationCount: 3,
    },
];

export const sampleMintedLink: MintEmbedLinkResponse = {
    token: "3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
    url: "https://acme-shop.myquill.ai/embed/3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
    expiresAt: "2026-06-16T20:00:00Z",
    maxInvocations: 100,
};
