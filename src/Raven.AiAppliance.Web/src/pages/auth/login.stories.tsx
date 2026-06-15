import type { Meta, StoryObj } from "@storybook/react-vite";
import { bootstrapMocks } from "@/mocks/bootstrap-mocks";
import { Login } from "./login";

const meta = {
    title: "Auth/Login",
    component: Login,
} satisfies Meta<typeof Login>;

export default meta;

type Story = StoryObj<typeof meta>;

export const LoggedOut: Story = {
    parameters: {
        msw: {
            handlers: {
                bootstrap: [bootstrapMocks.status({ state: "NeedsActivation" }), bootstrapMocks.redeemLicense()],
            },
        },
    },
};

export const Restarting: Story = {
    parameters: {
        msw: {
            handlers: {
                bootstrap: [
                    bootstrapMocks.status({ state: "Restarting" }),
                    bootstrapMocks.redeemLicense({ state: "Restarting" }),
                ],
            },
        },
    },
};
