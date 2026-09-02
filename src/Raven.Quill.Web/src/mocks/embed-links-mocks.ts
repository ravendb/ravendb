import type { ApiErrorResponse, EmbedLinkSummaryResponse, MintEmbedLinkResponse } from "@/api/generated/server-api";
import { SAMPLE_CHANNEL_ID } from "./channels-mocks";
import { apiHttp } from "./api-http";
import { MS_IN } from "@/lib/time";

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

const fromNow = (offsetMs: number) => new Date(Date.now() + offsetMs).toISOString();

// Dates are relative to "now" so the channel detail always shows the full spread of
// statuses: a healthy link, one expiring soon / nearing its limit, and an expired one.
export const sampleEmbedLinks: EmbedLinkSummaryResponse[] = [
    {
        token: "3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
        channelId: SAMPLE_CHANNEL_ID,
        agentId: "agents/sales",
        parameters: { customerId: "users/1" },
        createdAt: fromNow(-3 * MS_IN.day),
        expiresAt: fromNow(6 * MS_IN.day),
        maxInvocations: 100,
        invocationCount: 12,
    },
    {
        token: "9a8b7c6d5e4f3a2b1c0d9e8f7a6b5c4d",
        channelId: SAMPLE_CHANNEL_ID,
        agentId: "agents/sales",
        parameters: { customerId: "users/42" },
        createdAt: fromNow(-2 * MS_IN.day),
        expiresAt: fromNow(8 * MS_IN.hour),
        maxInvocations: 50,
        invocationCount: 44,
    },
    {
        token: "1c2d3e4f5a6b7c8d9e0f1a2b3c4d5e6f",
        channelId: SAMPLE_CHANNEL_ID,
        agentId: "agents/sales",
        parameters: { customerId: "users/108" },
        createdAt: fromNow(-10 * MS_IN.day),
        expiresAt: fromNow(-2 * MS_IN.day),
        maxInvocations: 200,
        invocationCount: 200,
    },
];

export const sampleMintedLink: MintEmbedLinkResponse = {
    token: "3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
    url: "https://public.myquill.ai/apps/acme-shop/embed/3f2a9c1b4d5e6f708192a3b4c5d6e7f8",
    expiresAt: "2026-06-16T20:00:00Z",
    maxInvocations: 100,
};
