import type { Meta, StoryObj } from "@storybook/react-vite";
import { aiConnectionStringsMocks } from "@/mocks/ai-connection-strings-mocks";
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

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [aiConnectionStringsMocks.list({ items: [] })],
            },
        },
    },
};
