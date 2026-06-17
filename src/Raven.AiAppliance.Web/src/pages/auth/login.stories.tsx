import type { Meta, StoryObj } from "@storybook/react-vite";
import { authMocks, authUnauthenticated } from "@/mocks/auth-mocks";
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
                auth: [authMocks.status(authUnauthenticated), authMocks.login()],
            },
        },
    },
};
