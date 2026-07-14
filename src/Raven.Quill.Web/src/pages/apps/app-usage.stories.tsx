import type { Meta, StoryObj } from "@storybook/react-vite";
import type { AppUsageResponse } from "@/api/generated/server-api";
import { statsMocks } from "@/mocks/stats-mocks";
import { AppUsage } from "./app-usage";

const meta = {
    title: "Apps/Usage",
    component: AppUsage,
    parameters: {
        page: { title: "Usage" },
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppUsage>;

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
        cdcWrites: zeroMetric,
    },
    tokensByCapability: emptySeries,
    tokensByModel: emptySeries,
    conversationsByChannel: emptySeries,
    cdcWrites: [],
    topTables: [],
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
