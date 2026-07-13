import type { Meta, StoryObj } from "@storybook/react-vite";
import { AppDataSource } from "./app-data-source";

const meta = {
    title: "Apps/Data Source",
    component: AppDataSource,
    parameters: {
        page: { title: "Data source" },
        // The detail mock only resolves known slugs, so start on a sample app.
        router: { initialPath: "/apps/demo", path: "/apps/:slug" },
    },
} satisfies Meta<typeof AppDataSource>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
