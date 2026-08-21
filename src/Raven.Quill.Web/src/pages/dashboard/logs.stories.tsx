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

type Canvas = ReturnType<typeof within>;

/**
 * Scoped lookups, because the read-only table repeats group names in its "Log" column. Every group is
 * a `section` labelled by its heading, so it resolves as a region.
 */
const getGroup = (canvas: Canvas, title: string | RegExp) => canvas.getByRole("region", { name: title });
const getRow = (canvas: Canvas, label: string) => canvas.getByText(label).closest('[data-slot="field"]') as HTMLElement;
const getReferenceTable = (canvas: Canvas) => within(getGroup(canvas, /Set in/)).getByRole("table");

// Radix renders a single-select ToggleGroup as role="group" with role="radio" items, so the level
// ladders are told apart by the group's accessible name rather than by index.
const getLevelLadder = (canvas: Canvas, name: string) => canvas.getByRole("group", { name });
const pickLevel = async (canvas: Canvas, group: string, level: string) =>
    userEvent.click(within(getLevelLadder(canvas, group)).getByRole("radio", { name: level }));

const getDirectoryInput = (canvas: Canvas) => canvas.getByLabelText("Log file directory");
// The label carries the pending count, so it is matched by prefix.
const getSaveButton = (canvas: Canvas) => canvas.getByRole("button", { name: /^Save / });

/** The page opens read-only; every control-bearing story has to ask for the controls first. */
async function startEditing(canvas: Canvas) {
    await userEvent.click(await waitFor(() => canvas.getByRole("button", { name: "Edit" })));
    await waitFor(() => expect(getLevelLadder(canvas, "Appliance minimum level")).toBeInTheDocument());
}

// Read before write: the page states its values and offers one action.
export const Default: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const group = await waitFor(() => getGroup(canvas, "Appliance log"));

        // No controls until asked for - each value is shown in a read-only box instead.
        expect(within(group).queryByRole("textbox")).not.toBeInTheDocument();
        expect(within(group).queryAllByRole("radio")).toHaveLength(0);
        expect(within(getRow(canvas, "Appliance minimum level")).getByText("Debug")).toBeInTheDocument();
        expect(within(getRow(canvas, "Log file directory")).getByText("/var/lib/quill/logs")).toBeInTheDocument();

        // Badges mark exceptions only, so a healthy page carries none.
        expect(canvas.queryAllByText(/^(Logging|Writing|stdout|Captured|Restart required)$/)).toHaveLength(0);
        expect(canvas.getByRole("button", { name: "Edit" })).toBeInTheDocument();
        expect(canvas.queryByRole("button", { name: /^Save / })).not.toBeInTheDocument();
        expect(canvas.queryByRole("button", { name: "Discard" })).not.toBeInTheDocument();
    },
};

// Edit swaps the values for controls and the button for the pair that ends editing.
export const EditingSwapsTheActions: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        expect(canvas.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
        expect(canvas.getByRole("button", { name: "Discard" })).toBeEnabled();
        // Nothing changed yet, so there is nothing to commit.
        expect(getSaveButton(canvas)).toBeDisabled();
        expect(getDirectoryInput(canvas)).toHaveValue("/var/lib/quill/logs");
        expect(canvas.getByRole("switch", { name: "Keep after restart" })).toBeInTheDocument();
    },
};

// The save button counts what is pending, so committing does not need a separate status line.
export const SaveButtonCountsThePendingChanges: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await pickLevel(canvas, "Appliance minimum level", "Warn");
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save 1 change" })).toBeEnabled());

        await userEvent.clear(getDirectoryInput(canvas));
        await userEvent.type(getDirectoryInput(canvas), "/var/log/quill");
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save 2 changes" })).toBeEnabled());

        // Discard restores the server's values and leaves edit mode.
        await userEvent.click(canvas.getByRole("button", { name: "Discard" }));
        await waitFor(() => expect(canvas.getByRole("button", { name: "Edit" })).toBeInTheDocument());
        expect(within(getRow(canvas, "Log file directory")).getByText("/var/lib/quill/logs")).toBeInTheDocument();
    },
};

// The shipped state: no log file, framework capture off, audit off. The appliance is still logging
// to stdout at Info, which the old layout reported as "Disabled".
export const NothingSwitchedOn: Story = {
    parameters: settings(settingsMocks.logConfiguration(sampleShippedLogConfiguration)),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await waitFor(() => expect(getGroup(canvas, "Appliance log")).toBeInTheDocument());

        // The appliance is logging, so its group stays badge-free.
        expect(canvas.queryByText("Logging")).not.toBeInTheDocument();
        expect(within(getRow(canvas, "Log file directory")).getByText("No file written")).toBeInTheDocument();
        // Both unavailable features are rows in the read-only table rather than cards of their own,
        // each flagged in its own Value cell instead of by a section-level alert.
        const table = getReferenceTable(canvas);
        expect(within(table).getByText("Not captured")).toBeInTheDocument();
        expect(within(table).getByText("No audit trail")).toBeInTheDocument();
        expect(canvas.getByText("To start an audit trail")).toBeInTheDocument();
    },
};

// The server answers 400 for a microsoftLogs block it cannot apply, so the control has to be gone
// rather than merely rejected - and with no level to set there is no group of its own either. Its state
// belongs with everything else only a file edit can change.
export const FrameworkCaptureOff: Story = {
    parameters: settings(settingsMocks.logConfiguration(sampleShippedLogConfiguration)),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        expect(canvas.queryByRole("group", { name: "Framework minimum level" })).not.toBeInTheDocument();
        // No group of its own while there is no level to set.
        expect(canvas.queryByText("Framework logging")).not.toBeInTheDocument();
        // Its state becomes a row in the read-only table instead.
        const table = getReferenceTable(canvas);
        expect(within(table).getByText("Framework")).toBeInTheDocument();
        expect(within(table).getByText("Not captured")).toBeInTheDocument();
        expect(canvas.getByText(/lower the/)).toBeInTheDocument();
    },
};

// Read-only reference is a table, not a card of label/value pairs: column headers name the columns,
// and nobody expects to type into a table cell.
export const ReadOnlyReferenceIsATable: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const section = await waitFor(() => getGroup(canvas, /Set in/));

        expect(section.closest('[data-slot="card"]')).toBeNull();
        const table = getReferenceTable(canvas);
        ["Log", "Setting", "Value"].forEach((header) =>
            expect(within(table).getByRole("columnheader", { name: header })).toBeInTheDocument(),
        );
        // No form controls anywhere in it.
        expect(within(table).queryByRole("textbox")).not.toBeInTheDocument();
        expect(within(table).queryAllByRole("radio")).toHaveLength(0);

        // Framework capture is on here, so it keeps its own editable card and stays out of the table.
        expect(canvas.getByText("Framework logging")).toBeInTheDocument();
        expect(within(table).queryByText("Framework")).not.toBeInTheDocument();
        expect(within(table).getByText("Filters")).toBeInTheDocument();
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
        await startEditing(canvas);

        const persistSwitch = canvas.getByRole("switch", { name: "Keep after restart" });
        expect(persistSwitch).toBeDisabled();
        expect(persistSwitch).not.toBeChecked();

        await pickLevel(canvas, "Appliance minimum level", "Warn");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(within(document.body).getByText("Log settings applied")).toBeInTheDocument());
        // A landed save returns the page to reading.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Edit" })).toBeInTheDocument());
    },
};

// A 500 means the live change landed and only the file write failed. It has to read as a
// warning, and the form must not stay dirty.
export const AppliedButNotSaved: Story = {
    parameters: settings(settingsMocks.updateLogConfigurationNotPersisted()),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await pickLevel(canvas, "Appliance minimum level", "Error");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(canvas.getByText("Applied but not saved")).toBeInTheDocument());
        // The appliance took it, so editing is over even though the file write failed.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Edit" })).toBeInTheDocument());
    },
};

// A rejected save changed nothing, so the operator's value has to survive for correcting - which
// means staying in edit mode.
export const SaveRejected: Story = {
    parameters: settings(settingsMocks.updateLogConfigurationError()),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await userEvent.clear(getDirectoryInput(canvas));
        await userEvent.type(getDirectoryInput(canvas), "/nope");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() => expect(canvas.getByText(/could not be created or written to/)).toBeInTheDocument());
        expect(getDirectoryInput(canvas)).toHaveValue("/nope");
        expect(canvas.queryByRole("button", { name: "Edit" })).not.toBeInTheDocument();
    },
};

// A relative directory resolves inside the container image, where the log file dies on the next
// recreate while the save still reports success. It has to be refused before it is sent.
export const RelativeDirectoryRefused: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await userEvent.clear(getDirectoryInput(canvas));
        await userEvent.type(getDirectoryInput(canvas), "logs");
        await userEvent.click(getSaveButton(canvas));

        await waitFor(() =>
            expect(canvas.getByText("Enter an absolute path, for example /var/lib/quill/logs")).toBeInTheDocument(),
        );
        // Refused in the browser, so nothing reached the appliance.
        expect(within(document.body).queryByText("Log settings saved")).not.toBeInTheDocument();

        await userEvent.clear(getDirectoryInput(canvas));
        await userEvent.type(getDirectoryInput(canvas), "/var/lib/quill/logs2");
        await userEvent.click(getSaveButton(canvas));
        await waitFor(() => expect(within(document.body).getByText("Log settings saved")).toBeInTheDocument());
    },
};

// Clearing the directory is the whole gesture for "stop writing logs", so it asks first.
export const SwitchingTheFileOffAsksFirst: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await userEvent.clear(getDirectoryInput(canvas));
        // The draft drives the page, so clearing the field immediately warns what saving would do.
        await waitFor(() => expect(canvas.getByText("Saving now switches the log file off")).toBeInTheDocument());
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

// Off silences stdout too. The old layout showed a green "Active" pill directly above the warning
// saying nothing is written, so every state on the page now follows the draft.
export const AllLoggingOff: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        await startEditing(canvas);

        await pickLevel(canvas, "Appliance minimum level", "Off");

        await waitFor(() => expect(canvas.getByText("Off silences the console too")).toBeInTheDocument());
        // The alert states the Off case in full, so the group carries no badge repeating it. The
        // directory field still earns one: it holds a path that is not being written to.
        expect(canvas.queryByText("Nothing logged")).not.toBeInTheDocument();
        expect(within(getRow(canvas, "Log file directory")).getByText("Silenced")).toBeInTheDocument();
        // The console row states it in copy rather than adding a third badge for one cause.
        expect(canvas.getByText(/Nothing reaches stdout while the appliance minimum level is Off/)).toBeInTheDocument();
    },
};

// The running level has drifted from the one in the config file. Drift belongs to that one setting,
// so the action lives in its row - and being there at all is the whole status, so there is no badge
// beside it. Clicking it stages the boot level and opens the controls for review rather than saving.
export const LevelChangedSinceBoot: Story = {
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const levelRow = await waitFor(() => getRow(canvas, "Appliance minimum level"));

        const revert = within(levelRow).getByRole("button", { name: "Revert to Info" });
        expect(within(levelRow).getByText("Debug")).toBeInTheDocument();
        // Its presence is the status; no separate "changed since boot" chip.
        expect(canvas.queryByText("Changed since boot")).not.toBeInTheDocument();

        await userEvent.click(revert);
        // Opens edit mode with the boot level staged, ready to review.
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save 1 change" })).toBeEnabled());
        expect(
            within(getLevelLadder(canvas, "Appliance minimum level")).getByRole("radio", { name: "Info" }),
        ).toBeChecked();
        // Redundant while editing - the ladder already offers that level.
        expect(canvas.queryByRole("button", { name: "Revert to Info" })).not.toBeInTheDocument();
    },
};

// The framework logger reports a booted level too, and the old layout printed it as a permanent
// "At startup" fact. Dropping that fact in the revamp lost it entirely, so it now drifts the same way
// the appliance level does.
export const FrameworkLevelChangedSinceBoot: Story = {
    parameters: settings(
        settingsMocks.logConfiguration({
            ...sampleLogConfiguration,
            microsoftLogs: { currentMinLevel: "Error", minLevel: "Warn" },
        }),
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const frameworkRow = await waitFor(() => getRow(canvas, "Framework minimum level"));

        expect(within(frameworkRow).getByText("Booted at Warn")).toBeInTheDocument();
        expect(within(frameworkRow).getByText("Error")).toBeInTheDocument();

        // Reverting stages the framework field, not the appliance one.
        await userEvent.click(within(frameworkRow).getByRole("button", { name: "Revert to Warn" }));
        await waitFor(() => expect(canvas.getByRole("button", { name: "Save 1 change" })).toBeEnabled());
        expect(
            within(getLevelLadder(canvas, "Framework minimum level")).getByRole("radio", { name: "Warn" }),
        ).toBeChecked();
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
