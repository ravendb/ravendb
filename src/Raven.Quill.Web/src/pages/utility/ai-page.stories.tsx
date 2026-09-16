import type { Meta, StoryObj } from "@storybook/react-vite";
import { AiPage } from "./ai-page";

const meta = {
    title: "Utility/AI",
    component: AiPage,
    parameters: {
        page: { title: "AI" },
    },
} satisfies Meta<typeof AiPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
