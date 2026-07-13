import type { Meta, StoryObj } from "@storybook/react-vite";
import { statsMocks } from "@/mocks/stats-mocks";
import { DashboardHome } from "./dashboard-home";

const meta = {
    title: "Dashboard/Home",
    component: DashboardHome,
    parameters: {
        // DashboardHome renders its own "My apps" header (so the trial pill sits beside
        // it), so the shell decorator wraps it without a title of its own.
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
                    statsMocks.dashboard(),
                    statsMocks.dashboardApps([]),
                    statsMocks.usage(),
                    statsMocks.tokensByApp(),
                ],
            },
        },
    },
};
