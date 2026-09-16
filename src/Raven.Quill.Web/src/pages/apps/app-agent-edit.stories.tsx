import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppAgentEdit } from "./app-agent-edit";

const meta = {
    title: "Apps/Edit agent",
    component: AppAgentEdit,
    parameters: {
        page: { title: "Edit agent" },
        router: {
            initialPath: `/apps/demo/agents/${encodeURIComponent("agents/sales")}/edit`,
            path: "/apps/:slug/agents/:agentId/edit",
        },
    },
} satisfies Meta<typeof AppAgentEdit>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
