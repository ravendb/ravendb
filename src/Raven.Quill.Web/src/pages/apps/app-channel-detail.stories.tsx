import type { Meta, StoryObj } from "@storybook/react-vite";
import { SAMPLE_CHANNEL_ID, SAMPLE_TELEGRAM_CHANNEL_ID } from "@/mocks/channels-mocks";
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
