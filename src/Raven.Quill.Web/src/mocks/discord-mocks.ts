import type { DiscordChannelHealthResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";
import { SAMPLE_DISCORD_CHANNEL_ID } from "./channels-mocks";

const HEALTHY: DiscordChannelHealthResponse = {
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
};

export const sampleDiscordHealth: DiscordChannelHealthResponse[] = [HEALTHY];

// Variants for developing the destructive/idle states the happy-path row never exercises.
export const pausedDiscordHealth: DiscordChannelHealthResponse[] = [
    { ...HEALTHY, enabled: false, gatewayConnected: false, lastConnectedAt: null },
];

export const tokenRejectedDiscordHealth: DiscordChannelHealthResponse[] = [
    {
        ...HEALTHY,
        gatewayConnected: false,
        lastConnectedAt: null,
        tokenValid: false,
        tokenError: "401: invalid bot token",
    },
];

export const gatewayDisconnectedDiscordHealth: DiscordChannelHealthResponse[] = [
    {
        ...HEALTHY,
        gatewayConnected: false,
        lastGatewayError: "Gateway closed (4004): authentication failed",
        lastSendErrorAt: "2026-08-21T12:06:00Z",
        lastSendError: "Cannot send messages to this user",
    },
];

export const discordMocks = {
    health: (rows: DiscordChannelHealthResponse[] = sampleDiscordHealth) =>
        apiHttp.get("/api/apps/{slug}/discord/health", ({ response }) => response(200).json(rows)),
};
