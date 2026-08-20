import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, waitFor, within } from "storybook/test";
import {
    SAMPLE_CHANNEL_ID,
    SAMPLE_TELEGRAM_CHANNEL_ID,
    SAMPLE_WHATSAPP_CHANNEL_ID,
    sampleWhatsAppConnected,
    sampleWhatsAppLoggedOut,
    sampleWhatsAppPairing,
    sampleWhatsAppPairingCode,
    whatsAppMocks,
} from "@/mocks/channels-mocks";
import { embedLinksMocks } from "@/mocks/embed-links-mocks";
import { AppChannelDetail } from "./app-channel-detail";

const meta = {
    title: "Apps/Channel detail",
    component: AppChannelDetail,
    parameters: {
        page: { title: "Channel" },
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
    },
} satisfies Meta<typeof AppChannelDetail>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const NoLinks: Story = {
    parameters: {
        msw: {
            handlers: {
                embedLinks: [embedLinksMocks.list([])],
            },
        },
    },
};

export const Telegram: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_TELEGRAM_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
    },
};

const whatsAppRouter = {
    initialPath: `/apps/demo/channels/${SAMPLE_WHATSAPP_CHANNEL_ID}`,
    path: "/apps/:slug/channels/:channelId",
};

export const WhatsAppPairing: Story = {
    parameters: { router: whatsAppRouter },
};

export const WhatsAppConnected: Story = {
    parameters: {
        router: whatsAppRouter,
        msw: {
            handlers: {
                whatsapp: [
                    whatsAppMocks.pairing(sampleWhatsAppConnected),
                    whatsAppMocks.pairingRestart(),
                    whatsAppMocks.health(),
                ],
            },
        },
    },
};

export const WhatsAppPairingCode: Story = {
    parameters: {
        router: whatsAppRouter,
        msw: {
            handlers: {
                whatsapp: [
                    whatsAppMocks.pairing(sampleWhatsAppPairingCode),
                    whatsAppMocks.pairingRestart(sampleWhatsAppPairingCode),
                    whatsAppMocks.health(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText(sampleWhatsAppPairingCode.pairingCode!)).toBeInTheDocument());
        expect(canvas.queryByRole("img", { name: /whatsapp pairing qr code/i })).not.toBeInTheDocument();
    },
};

export const WhatsAppLoggedOut: Story = {
    parameters: {
        router: whatsAppRouter,
        msw: {
            handlers: {
                whatsapp: [
                    whatsAppMocks.pairing(sampleWhatsAppLoggedOut),
                    whatsAppMocks.pairingRestart(),
                    whatsAppMocks.health(),
                ],
            },
        },
    },
};

export const WhatsAppPairingCompletes: Story = {
    parameters: {
        router: whatsAppRouter,
        msw: {
            handlers: {
                whatsapp: [
                    whatsAppMocks.pairingSequence([sampleWhatsAppPairing, sampleWhatsAppConnected]),
                    whatsAppMocks.pairingRestart(),
                    whatsAppMocks.health(),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByRole("img", { name: /whatsapp pairing qr code/i })).toBeInTheDocument());
        await waitFor(() => expect(canvas.getByText(sampleWhatsAppConnected.phoneNumber!)).toBeInTheDocument(), {
            timeout: 10_000,
        });
    },
};
