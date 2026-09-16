import type { Meta, StoryObj } from "@storybook/react-vite";
import { SimpleInfoPage } from "./simple-info-page";

const meta = {
    title: "Utility/Info Page",
    component: SimpleInfoPage,
    args: {
        title: "Docs",
        description: "Open RavenDB documentation from the top navigation.",
    },
    parameters: {
        page: { title: "Docs" },
    },
} satisfies Meta<typeof SimpleInfoPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
