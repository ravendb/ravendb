import type { Meta, StoryObj } from "@storybook/react-vite";
import { statsMocks } from "@/mocks/stats-mocks";
import { mockHandlers } from "@/mocks/default-mocks";
import { DashboardHome } from "./dashboard-home";

const meta = {
    title: "Dashboard/Home",
    component: DashboardHome,
    parameters: {
        page: {},
    },
} satisfies Meta<typeof DashboardHome>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        // Overriding a service replaces its whole handler array, so keep the stat-card
        // endpoints and only swap the apps list for an empty one.
        msw: mockHandlers({
            stats: [statsMocks.dashboardApps([]), statsMocks.usage(), statsMocks.tokensByApp()],
        }),
    },
};
