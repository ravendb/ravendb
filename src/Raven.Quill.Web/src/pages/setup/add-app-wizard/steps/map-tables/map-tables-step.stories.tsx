import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { createEmptyRootTable } from "@/pages/setup/add-app-wizard/steps/map-tables/map-tables-utils";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";

const meta = {
    title: "Setup/Add App Wizard/Map Schema",
    parameters: {
        page: { bare: true },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {
    render: () => <AppWizardAtStep initialStep="mapTables" />,
};

// The raw editor can only be left through a mapping that validates, so an incomplete table (here a
// just-added empty one) must not be able to open it - that used to be a one-way trip into raw JSON.
export const RawViewBlockedByInvalidTable: Story = {
    tags: ["!dev"],
    render: () => (
        <AppWizardAtStep
            initialStep="mapTables"
            seedOverride={(seed) => ({
                ...seed,
                mapTables: { tables: [...seed.mapTables.tables, createEmptyRootTable()] },
            })}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("switch", { name: /raw json/i }));

        await waitFor(() => expect(canvas.getByRole("switch", { name: /raw json/i })).not.toBeChecked());
        expect(canvas.getByPlaceholderText(/filter tables/i)).toBeInTheDocument();
    },
};

export const UnselectedTable: Story = {
    render: () => (
        <AppWizardAtStep
            initialStep="mapTables"
            seedOverride={(seed) => ({
                ...seed,
                verifySchema: { tables: [{ sourceTableSchema: "dbo", sourceTableName: "Customers" }] },
            })}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        expect(canvas.queryByText("1 deselected table will still be synced")).toBeInTheDocument();
    },
};
