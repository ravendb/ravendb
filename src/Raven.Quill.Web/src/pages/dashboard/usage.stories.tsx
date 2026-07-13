import type { Meta, StoryObj } from "@storybook/react-vite";
import { settingsMocks } from "@/mocks/settings-mocks";
import { DashboardUsage } from "./usage";

const meta = {
    title: "Dashboard/Usage",
    component: DashboardUsage,
    parameters: {
        // DashboardUsage renders its own "Usage" header with the month picker beside
        // it, so the shell decorator wraps it without a title of its own.
        page: {},
    },
} satisfies Meta<typeof DashboardUsage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            // Overriding a service replaces its whole handler array, so keep the
            // license endpoint and only swap usage for an empty month.
            handlers: {
                settings: [settingsMocks.license(), settingsMocks.usage({ byPeriod: [], perApplication: [] })],
            },
        },
    },
};
