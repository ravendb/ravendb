import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppSettings } from "./app-settings";

const meta = {
    title: "Apps/Settings",
    component: AppSettings,
    parameters: {
        page: { title: "Settings" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppSettings>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
