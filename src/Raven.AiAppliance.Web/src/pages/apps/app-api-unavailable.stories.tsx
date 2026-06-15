import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppApiUnavailable } from "./app-api-unavailable";

const meta = {
    title: "Apps/Feature Unavailable",
    component: AppApiUnavailable,
    args: { feature: "GenAI" },
    parameters: {
        page: { title: "GenAI" },
    },
} satisfies Meta<typeof AppApiUnavailable>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
