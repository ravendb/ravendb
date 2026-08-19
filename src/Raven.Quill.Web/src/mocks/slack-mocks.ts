import type { SlackChannelHealthResponse, SlackWebhookInfoResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";
import { SAMPLE_SLACK_CHANNEL_ID } from "./channels-mocks";

export const sampleSlackWebhookInfo: SlackWebhookInfoResponse = {
    requestUrl: "https://public.acme.myquill.ai/webhooks/slack/8b9c0d1e2f3a4b5c6d7e8f9012304a5b",
};

export const sampleSlackHealth: SlackChannelHealthResponse[] = [
    {
        channelId: SAMPLE_SLACK_CHANNEL_ID,
        teamId: "T0123456789",
        teamName: "Acme Coffee",
        botUserId: "U0QUILLBOT1",
        enabled: true,
        tokenValid: true,
        tokenError: null,
        lastInboundAt: "2026-08-16T08:12:00Z",
        lastSignatureFailureAt: null,
        lastSendErrorAt: null,
        lastSendError: null,
    },
];

export const slackMocks = {
    webhookInfo: (info: SlackWebhookInfoResponse = sampleSlackWebhookInfo) =>
        apiHttp.get("/api/apps/{slug}/channels/{channelId}/slack/webhook", ({ response }) => response(200).json(info)),
    health: (rows: SlackChannelHealthResponse[] = sampleSlackHealth) =>
        apiHttp.get("/api/apps/{slug}/slack/health", ({ response }) => response(200).json(rows)),
};
