import type { Meta, StoryObj } from "@storybook/react-vite";
import { appsMocks } from "@/mocks/apps-mocks";
import { DashboardHome } from "./dashboard-home";

const meta = {
    title: "Dashboard/Home",
    component: DashboardHome,
    parameters: {
        page: { title: "My apps" },
    },
} satisfies Meta<typeof DashboardHome>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [appsMocks.list([])],
            },
        },
    },
};
