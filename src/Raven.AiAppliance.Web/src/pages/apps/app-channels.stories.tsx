import type { Meta, StoryObj } from "@storybook/react-vite";
import { channelsMocks } from "@/mocks/channels-mocks";
import { AppChannels } from "./app-channels";

const meta = {
    title: "Apps/Channels",
    component: AppChannels,
    parameters: {
        page: { title: "Channels" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppChannels>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                channels: [channelsMocks.list([])],
            },
        },
    },
};
