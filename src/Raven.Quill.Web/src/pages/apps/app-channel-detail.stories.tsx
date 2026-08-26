import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import {
    SAMPLE_CHANNEL_ID,
    SAMPLE_DISCORD_CHANNEL_ID,
    SAMPLE_SLACK_CHANNEL_ID,
    SAMPLE_TELEGRAM_CHANNEL_ID,
} from "@/mocks/channels-mocks";
import { discordMocks, sampleDiscordHealth } from "@/mocks/discord-mocks";
import { embedLinksMocks } from "@/mocks/embed-links-mocks";
import { sampleSlackHealth, slackMocks } from "@/mocks/slack-mocks";
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

export const Slack: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_SLACK_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
    },
};

export const SlackTokenRejected: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_SLACK_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                slack: [
                    slackMocks.webhookInfo(),
                    slackMocks.health([
                        {
                            ...sampleSlackHealth[0],
                            tokenValid: false,
                            tokenError: "slack rejected the bot token",
                            lastInboundAt: null,
                            lastSignatureFailureAt: new Date().toISOString(),
                        },
                    ]),
                ],
            },
        },
    },
};

export const SlackSendError: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_SLACK_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                slack: [
                    slackMocks.webhookInfo(),
                    slackMocks.health([
                        {
                            ...sampleSlackHealth[0],
                            lastSendErrorAt: new Date().toISOString(),
                            lastSendError: "channel_not_found",
                        },
                    ]),
                ],
            },
        },
    },
};

export const Discord: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
    },
};

export const DiscordGatewayDisconnected: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                discord: [
                    discordMocks.health([
                        {
                            ...sampleDiscordHealth[0],
                            gatewayConnected: false,
                            lastGatewayError: "discord rejected the direct messages intent for this app",
                            lastInboundAt: null,
                        },
                    ]),
                ],
            },
        },
    },
};

export const DiscordParameters: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(await canvas.findByRole("tab", { name: /parameters/i }));

        const source = await canvas.findByRole("combobox", { name: "discordUser" });
        await waitFor(() => expect(source).toHaveTextContent(/sender discord user id/i));
        expect(canvas.getByText(/numeric discord user id/i)).toBeInTheDocument();
    },
};

export const DiscordSendErrorOlderThanTheLastMessage: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                discord: [
                    discordMocks.health([
                        {
                            ...sampleDiscordHealth[0],
                            lastSendErrorAt: "2026-08-21T11:50:00Z",
                            lastSendError: "503: discord is unavailable",
                        },
                    ]),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await canvas.findByText(/last message/i);
        expect(canvas.queryByText(/could not be delivered/i)).not.toBeInTheDocument();
    },
};

export const DiscordConnecting: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                discord: [
                    discordMocks.health([
                        {
                            ...sampleDiscordHealth[0],
                            gatewayConnected: false,
                            lastConnectedAt: null,
                            lastGatewayError: null,
                            lastInboundAt: null,
                        },
                    ]),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await canvas.findByText(/connecting/i);
        expect(canvas.queryByText(/gateway disconnected/i)).not.toBeInTheDocument();
    },
};

export const DiscordSendErrorNewerThanTheLastMessage: Story = {
    parameters: {
        router: {
            initialPath: `/apps/demo/channels/${SAMPLE_DISCORD_CHANNEL_ID}`,
            path: "/apps/:slug/channels/:channelId",
        },
        msw: {
            handlers: {
                discord: [
                    discordMocks.health([
                        {
                            ...sampleDiscordHealth[0],
                            lastSendErrorAt: "2026-08-21T12:10:00Z",
                            lastSendError: "503: discord is unavailable",
                        },
                    ]),
                ],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await canvas.findByText(/could not be delivered/i);
    },
};
