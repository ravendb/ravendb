import type { Meta, StoryObj } from "@storybook/react-vite";
import type { RequestHandler } from "msw";
import { expect, userEvent, waitFor, within } from "storybook/test";
import { defaultApiMocks } from "@/mocks/default-mocks";
import { sampleLogConfiguration, sampleShippedLogConfiguration, settingsMocks } from "@/mocks/settings-mocks";
import { DashboardLogs } from "./logs";

// Overriding a service replaces its whole array, and msw takes the first match, so an
// override goes in front of the defaults rather than re-listing them.
function settings(...overrides: RequestHandler[]) {
    return { msw: { handlers: { settings: [...overrides, ...defaultApiMocks.settings] } } };
}

const meta = {
    title: "Dashboard/Logs",
    component: DashboardLogs,
    parameters: {
        // The page renders its own "Logs" header, so the shell decorator adds no title.
        page: {},
    },
} satisfies Meta<typeof DashboardLogs>;

export default meta;

type Story = StoryObj<typeof meta>;

const getLevelSelect = (canvas: ReturnType<typeof within>) =>
    canvas.getAllByRole("combobox", { name: "Minimum level" })[0];
const getDirectoryInput = (canvas: ReturnType<typeof within>) => canvas.getByLabelText("Log file directory");
const getSaveButton = (canvas: ReturnType<typeof within>) => canvas.getByRole("button", { name: "Save changes" });

async function pickLevel(level: string) {
    const listbox = await waitFor(() => within(document.body).getByRole("listbox"));
    await userEvent.click(within(listbox).getByRole("option", { name: level }));
}

export const Default: Story = {};

// The shipped state: no log file, framework capture off, audit off.
export const NothingSwitchedOn: Story = {
    parameters: settings(settingsMocks.logConfiguration(sampleShippedLogConfiguration)),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(getDirectoryInput(canvas)).toHaveValue(""));
        expect(canvas.getAllByText("Disabled").length).toBeGreaterThan(0);
        expect(canvas.getByText("Not captured")).toBeInTheDocument();
    },
};

// The server answers 400 for a microsoftLogs block it cannot apply, so the control has to be
// gone rather than merely rejected.
export const FrameworkCaptureOff: Story = {
    parameters: settings(settingsMocks.logConfiguration(sampleShippedLogConfiguration)),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText("Not captured")).toBeInTheDocument());
        expect(canvas.getAllByRole("combobox", { name: "Minimum level" })).toHaveLength(1);
        expect(canvas.getByText(/Lower the/)).toBeInTheDocument();
    },
};

// canPersist false is the one condition that produces a 409, so the switch must be off and
// disabled while a live-only save still works.
export const CannotPersist: Story = {
    parameters: settings(
        settingsMocks.logConfiguration({ ...sampleLogConfiguration, canPersist: false }),
        settingsMocks.updateLogConfiguration(),
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const persistSwitch = await waitFor(() => canvas.getByRole("switch", { name: "Save to quill.nlog.config" }));
        expect(persistSwitch).toBeDisabled();
        expect(persistSwitch).not.toBeChecked();

        await userEvent.click(getLevelSelect(canvas));
        await pickLevel("Warn");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(within(document.body).getByText("Log settings applied")).toBeInTheDocument());
    },
};

// A 500 means the live change landed and only the file write failed. It has to read as a
// warning, and the form must not stay dirty.
export const AppliedButNotSaved: Story = {
    parameters: settings(settingsMocks.updateLogConfigurationNotPersisted()),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(getLevelSelect(canvas)).toBeInTheDocument());

        await userEvent.click(getLevelSelect(canvas));
        await pickLevel("Error");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(canvas.getByText("Applied but not saved")).toBeInTheDocument());
        // Reseeded from the server, so there is nothing left to save.
        await waitFor(() => expect(getSaveButton(canvas)).toBeDisabled());
    },
};

// A rejected save changed nothing, so the operator's value has to survive for correcting.
export const SaveRejected: Story = {
    parameters: settings(settingsMocks.updateLogConfigurationError()),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const directory = await waitFor(() => getDirectoryInput(canvas));

        await userEvent.clear(directory);
        await userEvent.type(directory, "/nope");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(canvas.getByText(/could not be created or written to/)).toBeInTheDocument());
        expect(getDirectoryInput(canvas)).toHaveValue("/nope");
    },
};

// A relative directory resolves inside the container image, where the log file dies on the next
// recreate while the save still reports success. It has to be refused before it is sent.
export const RelativeDirectoryRefused: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const directory = await waitFor(() => getDirectoryInput(canvas));

        await userEvent.clear(directory);
        await userEvent.type(directory, "logs");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() =>
            expect(canvas.getByText("Enter an absolute path, for example /var/lib/quill/logs")).toBeInTheDocument(),
        );
        // Refused in the browser, so nothing reached the appliance.
        expect(within(document.body).queryByText("Log settings saved")).not.toBeInTheDocument();

        await userEvent.clear(directory);
        await userEvent.type(directory, "/var/lib/quill/logs2");
        await userEvent.click(getSaveButton(canvas));
        await waitFor(() => expect(within(document.body).getByText("Log settings saved")).toBeInTheDocument());
    },
};

// Clearing the directory is the whole gesture for "stop writing logs", so it asks first.
export const SwitchingTheFileOffAsksFirst: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const directory = await waitFor(() => getDirectoryInput(canvas));

        await userEvent.clear(directory);
        await userEvent.click(getSaveButton(canvas));

        const dialog = await waitFor(() => within(document.body).getByRole("alertdialog"));
        await userEvent.click(within(dialog).getByRole("button", { name: "Keep the file" }));
        await waitFor(() => expect(within(document.body).queryByRole("alertdialog")).not.toBeInTheDocument());
        expect(within(document.body).queryByText("Log settings saved")).not.toBeInTheDocument();

        await userEvent.click(getSaveButton(canvas));
        const confirmDialog = await waitFor(() => within(document.body).getByRole("alertdialog"));
        await userEvent.click(within(confirmDialog).getByRole("button", { name: "Switch it off" }));

        await waitFor(() => expect(within(document.body).getByText("Log settings saved")).toBeInTheDocument());
    },
};

// Off silences stdout too, which is the one level worth warning about before it is saved.
export const AllLoggingOff: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(getLevelSelect(canvas)).toBeInTheDocument());

        await userEvent.click(getLevelSelect(canvas));
        await pickLevel("Off");

        await waitFor(() => expect(canvas.getByText("Off stops all appliance logging")).toBeInTheDocument());
    },
};

export const LoadError: Story = {
    parameters: settings(settingsMocks.logConfigurationError()),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(canvas.getByText("Could not load log settings")).toBeInTheDocument());
        expect(canvas.getByRole("button", { name: "Retry" })).toBeInTheDocument();
    },
};
