import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { failedMappingTest, setupMocks } from "@/mocks/setup-mocks";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";

const meta = {
    title: "Setup/Add App Wizard/Preview",
    parameters: {
        page: { bare: true },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {
    render: () => <AppWizardAtStep initialStep="preview" />,
    // Export is a footer action, sharing the completion button's group.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const exportButton = canvas.getByRole("button", { name: /export configuration/i });

        expect(exportButton.parentElement).toContainElement(canvas.getByRole("button", { name: /create app/i }));
    },
};

// The mapping ran but reported errors, so the preview shows the destructive banner instead of documents.
export const MappingErrors: Story = {
    parameters: {
        msw: { handlers: { setup: [setupMocks.testMapping(failedMappingTest), ...defaultApiMocks.setup] } },
    },
    render: () => <AppWizardAtStep initialStep="preview" />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await waitFor(() => expect(canvas.getByText("Testing the mapping failed")).toBeInTheDocument());

        // The banner is one alert, not an alert nested in another one.
        expect(canvasElement.querySelectorAll('[data-slot="alert"]')).toHaveLength(1);

        await userEvent.click(canvas.getByRole("button", { name: /show details/i }));
        expect(canvas.getByText(/is an invalid start of a value/i)).toBeInTheDocument();
    },
};
