import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";

const meta = {
    title: "Setup/Add App Wizard/Choose Data Source",
    parameters: {
        page: { bare: true },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const ChooseDataSource: Story = {
    render: () => <AppWizardAtStep initialStep="dataSource" />,
};
