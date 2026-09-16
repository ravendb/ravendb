import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { sampleApps } from "@/mocks/apps-mocks";
import { Button } from "@/components/shadcn/ui/button";
import { DeleteAppDialog } from "./delete-app-dialog";

const APP = sampleApps[0]!;

// Tests, not catalogue entries: `!dev` keeps these out of the Storybook sidebar while
// `pnpm test:storybook` still runs them. Play functions are the only way to exercise a
// component here — the unit project runs in node with no DOM.
const meta = {
    title: "Apps/DeleteAppDialog",
    component: DeleteAppDialog,
    tags: ["!dev"],
    args: {
        slug: APP.slug,
        appName: APP.name,
        trigger: <Button variant="destructive">Delete app</Button>,
    },
} satisfies Meta<typeof DeleteAppDialog>;

export default meta;

type Story = StoryObj<typeof meta>;

// The dialog portals out of the story root, so query the whole document body.
function openDialog(canvasElement: HTMLElement) {
    return userEvent.click(within(canvasElement).getByRole("button", { name: /delete app/i }));
}

function getConfirmButton() {
    return within(document.body).getByRole("button", { name: "Delete" });
}

function getConfirmationInput() {
    return within(document.body).getByLabelText(`Type ${APP.name} to confirm`);
}

function waitForDialogToClose() {
    return waitFor(() => expect(within(document.body).queryByRole("dialog")).not.toBeInTheDocument());
}

export const GateBlocksUntilExactName: Story = {
    play: async ({ canvasElement }) => {
        await openDialog(canvasElement);

        // Nothing typed yet, so the cascade stays behind the gate.
        await waitFor(() => expect(getConfirmButton()).toBeDisabled());

        await userEvent.type(getConfirmationInput(), APP.name.toLowerCase());
        expect(getConfirmButton()).toBeDisabled();

        await userEvent.clear(getConfirmationInput());
        await userEvent.type(getConfirmationInput(), APP.name);
        expect(getConfirmButton()).toBeEnabled();
    },
};

// Cancelling and deleting close the dialog by different routes — `onOpenChange` and the
// caller flipping `isOpen` — and neither may leave a satisfied gate behind.
export const EveryCloseClearsTheGate: Story = {
    play: async ({ canvasElement }) => {
        await openDialog(canvasElement);
        await userEvent.type(await waitFor(getConfirmationInput), APP.name);
        await waitFor(() => expect(getConfirmButton()).toBeEnabled());

        await userEvent.click(within(document.body).getByRole("button", { name: "Cancel" }));
        await waitForDialogToClose();

        await openDialog(canvasElement);
        await waitFor(() => expect(getConfirmationInput()).toHaveValue(""));
        expect(getConfirmButton()).toBeDisabled();

        await userEvent.type(getConfirmationInput(), APP.name);
        await userEvent.click(getConfirmButton());
        await waitForDialogToClose();
        await waitFor(() => expect(within(document.body).getByText(`App “${APP.name}” deleted`)).toBeInTheDocument());

        await openDialog(canvasElement);
        await waitFor(() => expect(getConfirmationInput()).toHaveValue(""));
        expect(getConfirmButton()).toBeDisabled();
    },
};
