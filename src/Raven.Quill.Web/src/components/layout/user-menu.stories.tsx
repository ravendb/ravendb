import type { Meta, StoryObj } from "@storybook/react-vite";
import { UserMenu } from "./user-menu";

const meta = {
    title: "Layout/User menu",
    component: UserMenu,
} satisfies Meta<typeof UserMenu>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
