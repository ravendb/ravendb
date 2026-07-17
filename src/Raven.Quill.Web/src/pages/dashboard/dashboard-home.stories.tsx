import type { Meta, StoryObj } from "@storybook/react-vite";
import { statsMocks } from "@/mocks/stats-mocks";
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
        msw: {
            // Overriding a service replaces its whole handler array, so keep the stat-card
            // endpoints and only swap the apps list for an empty one.
            handlers: {
                stats: [
                    statsMocks.dashboardApps([]),
                    statsMocks.usage(),
                    statsMocks.tokensByApp(),
                ],
            },
        },
    },
};
