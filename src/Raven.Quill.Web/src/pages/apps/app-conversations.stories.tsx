import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppConversations } from "./app-conversations";

const meta = {
    title: "Apps/Conversations",
    component: AppConversations,
    parameters: {
        page: { title: "Conversations" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppConversations>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
