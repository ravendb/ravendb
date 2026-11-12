import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, within } from "storybook/test";
import { defaultApiMocks, mockHandlers } from "@/mocks/default-mocks";
import { migrationMocks, plannerDiscovery, plannerDraft } from "@/mocks/migration-mocks";
import { setupMocks } from "@/mocks/setup-mocks";
import { AddAppWizard } from "@/pages/setup/add-app-wizard/add-app-wizard";

const meta = {
    title: "Setup/Add App Wizard/Full Wizard",
    component: AddAppWizard,
    parameters: {
        page: { bare: true },
    },
} satisfies Meta<typeof AddAppWizard>;

export default meta;

type Story = StoryObj<typeof meta>;

// The full wizard, mounted at the first step through the real entry component (form
// creation, store reset, and provision mutation wired up).
export const FullWizard: Story = {};

// The page reloaded on the planner step: the wizard continues there, with the plan and every value the
// server kept for the slug in the URL.
export const ResumedAfterReload: Story = {
    parameters: {
        router: { initialPath: "/app/add?draft=demo-shop" },
        msw: mockHandlers({
            setup: [
                setupMocks.draft({ ...plannerDraft, appliedPlanVersions: null }),
                setupMocks.discover(plannerDiscovery),
                migrationMocks.planGet(),
                ...defaultApiMocks.setup,
            ],
        }),
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await canvas.findByRole("heading", { name: "Plan your document model" });
        expect(await canvas.findByRole("region", { name: "Sales area" })).toBeInTheDocument();
    },
};
