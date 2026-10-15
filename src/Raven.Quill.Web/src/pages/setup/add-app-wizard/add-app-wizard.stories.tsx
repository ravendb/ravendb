import type { Meta, StoryObj } from "@storybook/react-vite";
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
