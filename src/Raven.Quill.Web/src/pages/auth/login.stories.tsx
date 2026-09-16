import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { authMocks, authUnauthenticated } from "@/mocks/auth-mocks";
import { Login } from "./login";

const meta = {
    title: "Auth/Login",
    component: Login,
    parameters: {
        msw: {
            handlers: {
                auth: [authMocks.status(authUnauthenticated), authMocks.login()],
            },
        },
    },
} satisfies Meta<typeof Login>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const InvalidKey: Story = {
    parameters: {
        msw: {
            handlers: {
                auth: [authMocks.status(authUnauthenticated), authMocks.loginInvalid()],
            },
        },
    },
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await userEvent.type(canvas.getByLabelText("Dashboard API key"), "wrong-key");
        await userEvent.click(canvas.getByRole("button", { name: /continue/i }));
        await waitFor(() => expect(canvas.getByRole("alert")).toBeInTheDocument());
    },
};
