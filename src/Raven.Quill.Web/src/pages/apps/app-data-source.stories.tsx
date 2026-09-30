import type { Meta, StoryObj } from "@storybook/react-vite";
import { appsMocks } from "@/mocks/apps-mocks";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { sampleDashboardApps, statsMocks } from "@/mocks/stats-mocks";
import { AppDataSource } from "./app-data-source";

// The page's own apps endpoints, minus the error list: a running sync is the default state, so each
// story picks the error list it wants and keeps the rest of the page live.
const appsWithoutErrorList = [appsMocks.detail(), appsMocks.cdcProgress(), appsMocks.cdcGet(), appsMocks.cdcRestart()];

const statsWithSyncErrors = [
    statsMocks.dashboardApp({ ...sampleDashboardApps[0], status: "error", statusSubtitle: "Sync errors detected" }),
    ...defaultApiMocks.stats,
];

const meta = {
    title: "Apps/Data source",
    component: AppDataSource,
    parameters: {
        page: { title: "Data source" },
        // The detail mock only resolves known slugs, so start on a sample app.
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
        msw: {
            handlers: {
                apps: [...appsWithoutErrorList, appsMocks.cdcErrors([])],
            },
        },
    },
} satisfies Meta<typeof AppDataSource>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const WithErrors: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [...appsWithoutErrorList, appsMocks.cdcErrors()],
                stats: statsWithSyncErrors,
            },
        },
    },
};
