import type { Meta, StoryObj } from "@storybook/react-vite";
import { bootstrapMocks } from "@/mocks/bootstrap-mocks";
import { BootGate } from "./boot-gate";

const meta = {
    title: "Auth/Boot gate",
    component: BootGate,
    args: {
        children: (
            <div className="grid min-h-svh place-items-center text-sm text-muted-foreground">Operator dashboard</div>
        ),
    },
} satisfies Meta<typeof BootGate>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Activating: Story = {
    parameters: {
        msw: {
            handlers: {
                bootstrap: [bootstrapMocks.status({ state: "Redeeming" })],
            },
        },
    },
};

export const Restarting: Story = {
    parameters: {
        msw: {
            handlers: {
                bootstrap: [bootstrapMocks.status({ state: "Restarting" })],
            },
        },
    },
};

export const Ready: Story = {
    parameters: {
        msw: {
            handlers: {
                bootstrap: [bootstrapMocks.status({ state: "Ready" })],
            },
        },
    },
};
