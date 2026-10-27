import type { Meta, StoryObj } from "@storybook/react-vite";
import type { AppUsageResponse } from "@/api/generated/server-api";
import { statsMocks } from "@/mocks/stats-mocks";
import { AppAnalytics } from "./app-analytics";

const meta = {
    title: "Apps/Analytics",
    component: AppAnalytics,
    parameters: {
        page: { title: "Analytics" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppAnalytics>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

const emptySeries = { points: [], keys: [] };
const zeroMetric = { value: 0, delta: 0, sparkline: [] };

// Fresh app with no traffic yet: zeroed stat cards, empty charts, and empty tables.
const emptyAppUsage: AppUsageResponse = {
    metrics: {
        conversations: zeroMetric,
        tokens: zeroMetric,
        buckets: [],
    },
    tokensByCapability: emptySeries,
    tokensByModel: emptySeries,
    conversationsByChannel: emptySeries,
    topCapabilities: [],
};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                stats: [statsMocks.appUsage(emptyAppUsage)],
            },
        },
    },
};
