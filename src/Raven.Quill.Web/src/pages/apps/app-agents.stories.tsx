import type { Meta, StoryObj } from "@storybook/react-vite";
import { agentsMocks } from "@/mocks/agents-mocks";
import { mockHandlers } from "@/mocks/default-mocks";
import { AppAgents } from "./app-agents";

const meta = {
    title: "Apps/Agents",
    component: AppAgents,
    parameters: {
        page: {},
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppAgents>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: mockHandlers({
            agents: [agentsMocks.list([])],
        }),
    },
};
