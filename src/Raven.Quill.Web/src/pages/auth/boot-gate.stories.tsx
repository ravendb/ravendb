import type { Meta, StoryObj } from "@storybook/react-vite";
import { bootstrapMocks } from "@/mocks/bootstrap-mocks";
import { mockHandlers } from "@/mocks/default-mocks";
import { Text } from "@/components/typography";
import { BootGate } from "./boot-gate";

const meta = {
    title: "Auth/Boot gate",
    component: BootGate,
    args: {
        children: (
            <Text as="div" variant="muted" className="grid min-h-svh place-items-center">
                Operator dashboard
            </Text>
        ),
    },
} satisfies Meta<typeof BootGate>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Activating: Story = {
    parameters: {
        msw: mockHandlers({
            bootstrap: [bootstrapMocks.status({ state: "Redeeming" })],
        }),
    },
};

export const Restarting: Story = {
    parameters: {
        msw: mockHandlers({
            bootstrap: [bootstrapMocks.status({ state: "Restarting" })],
        }),
    },
};

export const Ready: Story = {
    parameters: {
        msw: mockHandlers({
            bootstrap: [bootstrapMocks.status({ state: "Ready" })],
        }),
    },
};
