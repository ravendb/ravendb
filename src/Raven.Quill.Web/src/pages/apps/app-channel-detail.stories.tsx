import type { Meta, StoryObj } from "@storybook/react-vite";
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
                            lastGatewayError:
                                "the Direct Messages intent is not enabled for this app; turn it on under Bot > Privileged Gateway Intents in the Developer Portal",
                            lastInboundAt: null,
                        },
                    ]),
                ],
            },
        },
    },
};
