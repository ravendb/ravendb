import type { Meta, StoryObj } from "@storybook/react-vite";
import { aiConnectionStringsMocks } from "@/mocks/ai-connection-strings-mocks";
import { DashboardConnectionStrings } from "./connection-strings";

const meta = {
    title: "Dashboard/Connection strings",
    component: DashboardConnectionStrings,
    parameters: {
        // The page renders its own "AI connection strings" header with the add
        // button beside it, so the shell decorator adds no title.
        page: {},
    },
} satisfies Meta<typeof DashboardConnectionStrings>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Empty: Story = {
    parameters: {
        msw: {
            handlers: {
                aiConnectionStrings: [aiConnectionStringsMocks.list([])],
            },
        },
    },
};
