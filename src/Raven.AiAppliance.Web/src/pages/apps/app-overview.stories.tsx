import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppOverview } from "./app-overview";

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
