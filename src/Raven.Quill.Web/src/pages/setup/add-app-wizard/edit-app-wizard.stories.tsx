import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import { appsMocks, sampleAppCdcConfiguration } from "@/mocks/apps-mocks";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { EditAppWizard } from "./edit-app-wizard";

const meta = {
    title: "Setup/Edit App Wizard",
    component: EditAppWizard,
    parameters: {
        page: { bare: true },
        router: { initialPath: "/apps/acme-shop/edit", path: "/apps/:slug/edit" },
    },
} satisfies Meta<typeof EditAppWizard>;

export default meta;

type Story = StoryObj<typeof meta>;

// Seeded from the app's stored CDC configuration
export const Default: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        const slug = await canvas.findByLabelText("Public URL slug");
        expect(slug).toHaveValue("acme-shop");
        expect(slug).toBeDisabled();

        // The stored string parses cleanly, so the wizard seeds the connection details, not the raw string.
        const host = canvas.getByLabelText("Host");
        expect(host).toHaveValue("localhost");
        expect(host).toBeEnabled();
        expect(canvas.getByLabelText("Database")).toHaveValue("demo_shop");
        expect(canvas.getByLabelText("Username")).toHaveValue("admin");
    },
};

// A configuration written outside the wizard says why instead of opening an empty mapping.
export const UnsupportedConfiguration: Story = {
    parameters: {
        msw: {
            handlers: {
                apps: [
                    appsMocks.cdcGet({
                        configuration: { name: "cdc/acme-shop", tables: [] },
                        connectionString: sampleAppCdcConfiguration.connectionString,
                    }),
                    ...defaultApiMocks.apps,
                ],
            },
        },
    },
};
