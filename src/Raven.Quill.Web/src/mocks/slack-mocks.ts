import type { SlackChannelHealthResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";
import { SAMPLE_SLACK_CHANNEL_ID } from "./channels-mocks";

export const sampleSlackHealth: SlackChannelHealthResponse[] = [
    {
        channelId: SAMPLE_SLACK_CHANNEL_ID,
        teamId: "T0123456789",
        teamName: "Acme Coffee",
        botUserId: "U0QUILLBOT1",
        enabled: true,
        tokenValid: true,
        tokenError: null,
        socketConnected: true,
        lastConnectedAt: "2026-08-16T08:00:00Z",
        lastSocketError: null,
        lastInboundAt: "2026-08-16T08:12:00Z",
        lastSendErrorAt: null,
        lastSendError: null,
    },
];

export const slackMocks = {
    health: (rows: SlackChannelHealthResponse[] = sampleSlackHealth) =>
        apiHttp.get("/api/apps/{slug}/slack/health", ({ response }) => response(200).json(rows)),
};
