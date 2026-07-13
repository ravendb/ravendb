import type { Meta, StoryObj } from "@storybook/react-vite";
import { sampleLicense, settingsMocks } from "@/mocks/settings-mocks";
import { DashboardLicense } from "./license";

const meta = {
    title: "Dashboard/License",
    component: DashboardLicense,
    parameters: {
        // DashboardLicense renders its own "License" header with the refresh button
        // beside it, so the shell decorator wraps it without a title of its own.
        page: {},
    },
} satisfies Meta<typeof DashboardLicense>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Expired: Story = {
    parameters: {
        msw: {
            // Overriding a service replaces its whole handler array, so keep the
            // usage endpoint and only swap the license for an expired one.
            handlers: {
                settings: [
                    settingsMocks.license({
                        ...sampleLicense,
                        response: {
                            ...sampleLicense.response,
                            expired: true,
                            expiration: "2026-06-14T00:00:00Z",
                        },
                    }),
                    settingsMocks.usage(),
                ],
            },
        },
    },
};

export const ConnectivityIssue: Story = {
    parameters: {
        msw: {
            handlers: {
                settings: [
                    settingsMocks.license({
                        ...sampleLicense,
                        connectivity: {
                            statusCode: "ServiceUnavailable",
                            exception: "The licensing API did not respond within 30 seconds.",
                        },
                    }),
                    settingsMocks.usage(),
                ],
            },
        },
    },
};
