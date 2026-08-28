import type { DiscordChannelHealthResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";
import { SAMPLE_DISCORD_CHANNEL_ID } from "./channels-mocks";

export const sampleDiscordHealth: DiscordChannelHealthResponse[] = [
    {
        channelId: SAMPLE_DISCORD_CHANNEL_ID,
        applicationId: "412873098765432100",
        botUserId: "412873098765432100",
        botUsername: "acme-helper",
        enabled: true,
        tokenValid: true,
        tokenError: null,
        gatewayConnected: true,
        lastConnectedAt: "2026-08-21T11:41:00Z",
        lastGatewayError: null,
        lastInboundAt: "2026-08-21T12:05:00Z",
        lastSendErrorAt: null,
        lastSendError: null,
    },
];

export const discordMocks = {
    health: (rows: DiscordChannelHealthResponse[] = sampleDiscordHealth) =>
        apiHttp.get("/api/apps/{slug}/discord/health", ({ response }) => response(200).json(rows)),
};
