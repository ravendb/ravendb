import type { Meta, StoryObj } from "@storybook/react-vite";
import { channelsMocks } from "@/mocks/channels-mocks";
import { embedLinksMocks } from "@/mocks/embed-links-mocks";
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

// Open "Generate embed link" on the Website widget row and submit to see the
// server's inline error surfaced (e.g. a required parameter left blank server-side).
export const LinkMintError: Story = {
    parameters: {
        msw: {
            handlers: {
                embedLinks: [embedLinksMocks.mintError()],
            },
        },
    },
};
