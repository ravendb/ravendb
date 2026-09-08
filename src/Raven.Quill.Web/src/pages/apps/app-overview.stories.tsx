import type { Meta, StoryObj } from "@storybook/react-vite";
import { agentsMocks } from "@/mocks/agents-mocks";
import { appsMocks } from "@/mocks/apps-mocks";
import { channelsMocks } from "@/mocks/channels-mocks";
import { AppOverview } from "./app-overview";

const appsWithoutCdcErrors = [appsMocks.detail(), appsMocks.cdcErrors([])];

const meta = {
    title: "Apps/Overview",
    component: AppOverview,
    parameters: {
        page: { title: "Overview" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppOverview>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const WithoutCdcErrors: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: appsWithoutCdcErrors,
            },
        },
    },
};

// Fresh app: no agents or channels yet, so the welcome panel shows the remaining
// steps as numbered (incomplete) call-to-actions.
export const Onboarding: Story = {
    parameters: {
        msw: {
            handlers: {
                agents: [agentsMocks.list([])],
                apps: appsWithoutCdcErrors,
                channels: [channelsMocks.list([])],
            },
        },
    },
};
