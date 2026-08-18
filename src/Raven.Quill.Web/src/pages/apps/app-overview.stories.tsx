import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import { agentsMocks } from "@/mocks/agents-mocks";
import { appsMocks } from "@/mocks/apps-mocks";
import { channelsMocks } from "@/mocks/channels-mocks";
import { AppOverview } from "./app-overview";

const appsWithoutCdcErrors = [appsMocks.detail(), appsMocks.cdcPerformance(), appsMocks.cdcErrors([])];

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

// The card's own states live in Apps/Data Sync; this only checks it reaches the page.
export const Default: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        expect(await canvas.findByRole("heading", { name: /data sync/i })).toBeInTheDocument();
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
