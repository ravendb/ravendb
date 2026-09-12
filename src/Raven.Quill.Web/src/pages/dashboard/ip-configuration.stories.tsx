import type { Meta, StoryObj } from "@storybook/react-vite";
import { dnsMocks } from "@/mocks/dns-mocks";
import { DashboardIpConfiguration } from "./ip-configuration";

const meta = {
    title: "Dashboard/IP configuration",
    component: DashboardIpConfiguration,
    args: {
        hostname: "dashboard.acme.ravendb.run",
    },
    parameters: {
        // DashboardIpConfiguration renders its own "IP configuration" header with
        // the refresh button beside it, so the shell decorator adds no title.
        page: {},
    },
} satisfies Meta<typeof DashboardIpConfiguration>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const LookupFailed: Story = {
    parameters: {
        msw: {
            handlers: {
                dns: [dnsMocks.ipBindingError()],
            },
        },
    },
};

export const OpenedByIp: Story = {
    args: {
        hostname: "192.168.1.20",
    },
};
