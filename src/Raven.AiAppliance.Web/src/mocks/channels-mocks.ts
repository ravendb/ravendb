import type { ChannelSummaryResponse, ProvisionChannelResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const channelsMocks = {
    list: (channels: ChannelSummaryResponse[] = sampleChannels) =>
        apiHttp.get("/api/apps/{slug}/channels", ({ response }) => response(200).json(channels)),
    create: (result: ProvisionChannelResponse = { widgetId: SAMPLE_WEB_WIDGET_ID, existing: false }) =>
        apiHttp.post("/api/apps/{slug}/setup/channel", ({ response }) => response(200).json(result)),
    update: (channels: ChannelSummaryResponse[] = sampleChannels) =>
        apiHttp.put("/api/apps/{slug}/channels/{channelId}", async ({ params, request, response }) => {
            const channel = channels.find((candidate) => candidate.widgetId === params.channelId);

            if (!channel) {
                return response(404).json({ error: `Unknown channel: ${params.channelId}` });
            }

            const update = await request.json();

            return response(200).json({
                ...channel,
                displayName: update.displayName ?? channel.displayName,
                enabled: update.enabled ?? channel.enabled,
            });
        }),
    delete: () => apiHttp.delete("/api/apps/{slug}/channels/{channelId}", ({ response }) => response(204).empty()),
};

// Realistic, URL-safe channel ids (provisioning mints `wgt_<32 hex>`); the web
// id is shared with the embed-links mocks so the channel detail route resolves.
export const SAMPLE_WEB_WIDGET_ID = "wgt_4a1f9c2b7d8e4f6a9b0c1d2e3f405162";

export const sampleChannels: ChannelSummaryResponse[] = [
    {
        widgetId: SAMPLE_WEB_WIDGET_ID,
        type: "IFrame",
        agentId: "agents/sales",
        displayName: "Website widget",
        enabled: true,
        createdAt: "2026-05-03T09:00:00Z",
    },
    {
        widgetId: "tlg_2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e",
        type: "Telegram",
        agentId: "agents/faq",
        displayName: "Telegram bot",
        enabled: false,
        createdAt: "2026-05-09T14:20:00Z",
    },
];
