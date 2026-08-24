import type { ChannelSummaryResponse, ProvisionChannelResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const channelsMocks = {
    list: (channels: ChannelSummaryResponse[] = sampleChannels) =>
        apiHttp.get("/api/apps/{slug}/channels", ({ response }) => response(200).json(channels)),
    create: (result: ProvisionChannelResponse = { channelId: SAMPLE_CHANNEL_ID }) =>
        apiHttp.post("/api/apps/{slug}/setup/channel", ({ response }) => response(200).json(result)),
    update: (channels: ChannelSummaryResponse[] = sampleChannels) =>
        apiHttp.put("/api/apps/{slug}/channels/{channelId}", async ({ params, request, response }) => {
            const channel = channels.find((candidate) => candidate.channelId === params.channelId);

            if (!channel) {
                return response(404).json({ error: `Unknown channel: ${params.channelId}` });
            }

            const update = await request.json();

            return response(200).json({
                ...channel,
                displayName: update.displayName ?? channel.displayName,
                enabled: update.enabled ?? channel.enabled,
                allowedOrigins: update.allowedOrigins ?? channel.allowedOrigins,
                telegram: channel.telegram
                    ? {
                          ...channel.telegram,
                          messages: update.telegram?.messages ?? channel.telegram.messages,
                          parameterBindings: update.telegram?.parameterBindings ?? channel.telegram.parameterBindings,
                      }
                    : channel.telegram,
            });
        }),
    delete: () => apiHttp.delete("/api/apps/{slug}/channels/{channelId}", ({ response }) => response(204).empty()),
};

// Realistic, URL-safe channel ids (provisioning mints a 32-hex id); the web
// id is shared with the embed-links mocks so the channel detail route resolves.
export const SAMPLE_CHANNEL_ID = "4a1f9c2b7d8e4f6a9b0c1d2e3f405162";
export const SAMPLE_TELEGRAM_CHANNEL_ID = "tlg_2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e";
export const SAMPLE_SLACK_CHANNEL_ID = "slk_4d5e6f7a8b9c0d1e2f3a4b5c6d7e8f90";

export const sampleChannels: ChannelSummaryResponse[] = [
    {
        channelId: SAMPLE_CHANNEL_ID,
        type: "IFrame",
        agentId: "agents/sales",
        displayName: "Website widget",
        enabled: true,
        createdAt: "2026-05-03T09:00:00Z",
        allowedOrigins: ["https://example.com", "https://shop.example.com"],
    },
    {
        channelId: SAMPLE_TELEGRAM_CHANNEL_ID,
        type: "Telegram",
        agentId: "agents/faq",
        displayName: "Telegram bot",
        enabled: false,
        createdAt: "2026-05-09T14:20:00Z",
        allowedOrigins: [],
        telegram: {
            botUsername: "acme_faq_bot",
            parameterBindings: {
                company: { source: "Constant", value: "Acme Corp" },
                senderId: { source: "UserId", value: null },
                userHandle: { source: "Username", value: null },
                phoneNumber: { source: "PhoneNumber", value: null },
            },
            messages: null,
        },
    },
    {
        channelId: SAMPLE_SLACK_CHANNEL_ID,
        type: "Slack",
        agentId: "agents/support",
        displayName: "Acme workspace bot",
        enabled: true,
        createdAt: "2026-08-18T09:15:00Z",
        allowedOrigins: [],
        slack: {
            teamId: "T0123456789",
            teamName: "Acme Coffee",
            botUserId: "U0QUILLBOT1",
            parameterBindings: {
                slackUser: { source: "UserId", value: null },
            },
        },
    },
];
