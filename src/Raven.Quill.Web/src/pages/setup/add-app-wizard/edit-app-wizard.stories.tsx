import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import { appsMocks, sampleAppCdcConfiguration } from "@/mocks/apps-mocks";
import { defaultApiMocks, mockHandlers } from "@/mocks/default-mocks";
import { migrationMocks, plannerDraft } from "@/mocks/migration-mocks";
import { setupMocks } from "@/mocks/setup-mocks";
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
        msw: mockHandlers({
            apps: [
                appsMocks.cdcGet({
                    configuration: { name: "cdc/acme-shop", tables: [] },
                    connectionString: sampleAppCdcConfiguration.connectionString,
                }),
                ...defaultApiMocks.apps,
            ],
        }),
    },
};

// The planner applied this app's mapping and the plan still holds what was applied, so the plan reopens in
// place of the mapping editor.
export const PlanMadeApp: Story = {
    parameters: {
        msw: mockHandlers({
            setup: [
                setupMocks.draft({ ...plannerDraft, isAppCreated: true }),
                migrationMocks.planGet(),
                ...defaultApiMocks.setup,
            ],
        }),
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const steps = within(await canvas.findByLabelText("Setup steps"));

        expect(steps.getByText("Plan your document model")).toBeInTheDocument();
        expect(steps.queryByText("Map schema")).not.toBeInTheDocument();
    },
};

// The plan moved on after it was applied, so it no longer shows what the app runs on and the editor opens.
export const PlanChangedSinceApplied: Story = {
    parameters: {
        msw: mockHandlers({
            setup: [
                setupMocks.draft({ ...plannerDraft, isAppCreated: true, appliedPlanVersions: { Orders: 1 } }),
                migrationMocks.planGet(),
                ...defaultApiMocks.setup,
            ],
        }),
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const steps = within(await canvas.findByLabelText("Setup steps"));

        expect(steps.getByText("Map schema")).toBeInTheDocument();
        expect(steps.queryByText("Plan your document model")).not.toBeInTheDocument();
    },
};
