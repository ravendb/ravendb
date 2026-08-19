import type {
    ChannelSummaryResponse,
    ProvisionChannelResponse,
    WhatsAppChannelHealthResponse,
    WhatsAppPairingResponse,
} from "@/api/generated/server-api";
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
                telegram: channel.telegram
                    ? { ...channel.telegram, messages: update.telegram?.messages ?? channel.telegram.messages }
                    : channel.telegram,
            });
        }),
    delete: () => apiHttp.delete("/api/apps/{slug}/channels/{channelId}", ({ response }) => response(204).empty()),
};

export const whatsAppMocks = {
    health: (items: WhatsAppChannelHealthResponse[] = sampleWhatsAppHealth) =>
        apiHttp.get("/api/apps/{slug}/whatsapp/health", ({ response }) => response(200).json(items)),
    pairing: (pairing: WhatsAppPairingResponse = sampleWhatsAppPairing) =>
        apiHttp.get("/api/apps/{slug}/channels/{channelId}/whatsapp/pairing", ({ response }) =>
            response(200).json(pairing),
        ),
    pairingSequence: (responses: WhatsAppPairingResponse[]) => {
        let call = 0;
        return apiHttp.get("/api/apps/{slug}/channels/{channelId}/whatsapp/pairing", ({ response }) =>
            response(200).json(responses[Math.min(call++, responses.length - 1)]),
        );
    },
    pairingRestart: (pairing: WhatsAppPairingResponse = sampleWhatsAppPairing) =>
        apiHttp.post("/api/apps/{slug}/channels/{channelId}/whatsapp/pairing/restart", ({ response }) =>
            response(200).json(pairing),
        ),
};

// Realistic, URL-safe channel ids (provisioning mints a 32-hex id); the web
// id is shared with the embed-links mocks so the channel detail route resolves.
export const SAMPLE_CHANNEL_ID = "4a1f9c2b7d8e4f6a9b0c1d2e3f405162";
export const SAMPLE_TELEGRAM_CHANNEL_ID = "tlg_2b3c4d5e6f7a8b9c0d1e2f3a4b5c6d7e";
export const SAMPLE_WHATSAPP_CHANNEL_ID = "7f3e5a1c9b2d4e6f8a0b1c2d3e4f5061";

export const sampleChannels: ChannelSummaryResponse[] = [
    {
        channelId: SAMPLE_CHANNEL_ID,
        type: "IFrame",
        agentId: "agents/sales",
        displayName: "Website widget",
        enabled: true,
        createdAt: "2026-05-03T09:00:00Z",
    },
    {
        channelId: SAMPLE_TELEGRAM_CHANNEL_ID,
        type: "Telegram",
        agentId: "agents/faq",
        displayName: "Telegram bot",
        enabled: false,
        createdAt: "2026-05-09T14:20:00Z",
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
        channelId: SAMPLE_WHATSAPP_CHANNEL_ID,
        type: "WhatsAppPersonal",
        agentId: "agents/faq",
        displayName: "QA test phone",
        enabled: true,
        createdAt: "2026-07-21T10:00:00Z",
    },
];

export const sampleWhatsAppPairing: WhatsAppPairingResponse = {
    state: "Pairing",
    qr: "2@AbCdEfGhIjKlMnOpQrStUvWxYz0123456789+/=,ABCDEFabcdef0123456789,XyZ=",
    qrExpiresAt: "2026-07-21T10:01:00Z",
    pairingCode: null,
    phoneNumber: null,
    lastError: null,
};

export const sampleWhatsAppPairingCode: WhatsAppPairingResponse = {
    state: "Pairing",
    qr: null,
    qrExpiresAt: null,
    pairingCode: "4M7XK2QP",
    phoneNumber: null,
    lastError: null,
};

export const sampleWhatsAppConnected: WhatsAppPairingResponse = {
    state: "Connected",
    qr: null,
    qrExpiresAt: null,
    pairingCode: null,
    phoneNumber: "+48601234567",
    lastError: null,
};

export const sampleWhatsAppLoggedOut: WhatsAppPairingResponse = {
    state: "LoggedOut",
    qr: null,
    qrExpiresAt: null,
    pairingCode: null,
    phoneNumber: null,
    lastError: "the phone unlinked this device",
};

export const sampleWhatsAppHealth: WhatsAppChannelHealthResponse[] = [
    {
        channelId: SAMPLE_WHATSAPP_CHANNEL_ID,
        phoneNumber: null,
        enabled: true,
        state: "Pairing",
        lastError: null,
    },
];
