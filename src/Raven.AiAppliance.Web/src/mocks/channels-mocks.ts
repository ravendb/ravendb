import type { ChannelSummaryResponse, ProvisionChannelResponse } from "@/api/generated/server-api";
import { apiHttp } from "./api-http";

export const channelsMocks = {
    list: (channels: ChannelSummaryResponse[] = sampleChannels) =>
        apiHttp.get("/api/apps/{slug}/channels", ({ response }) => response(200).json(channels)),
    create: (result: ProvisionChannelResponse = { widgetId: "widgets/web", existing: false }) =>
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

export const sampleChannels: ChannelSummaryResponse[] = [
    {
        widgetId: "widgets/web",
        type: "IFrame",
        agentId: "agents/sales",
        displayName: "Website widget",
        enabled: true,
        createdAt: "2026-05-03T09:00:00Z",
    },
    {
        widgetId: "widgets/telegram",
        type: "Telegram",
        agentId: "agents/faq",
        displayName: "Telegram bot",
        enabled: false,
        createdAt: "2026-05-09T14:20:00Z",
    },
];
