import type { Meta, StoryObj } from "@storybook/react-vite";
import { expect, userEvent, waitFor, within } from "storybook/test";
import type { DiscoverResponse } from "@/api/generated/server-api";
import { RANGE_PREVIEW_ROW_CLASSNAME } from "@/components/table/row-range-selection";
import { defaultApiMocks, discoverHandlers } from "@/mocks/default-mocks";
import { plannerOpeningFrames, PLANNER_CONVERSATION_ID } from "@/mocks/migration-mocks";
import {
    discoveryWithAllStates,
    failedCdcVerification,
    failedDiscovery,
    manyTablesDiscovery,
    sampleDiscovery,
    setupMocks,
} from "@/mocks/setup-mocks";
import { useSetupWizardStore } from "@/pages/setup/add-app-wizard/app-wizard-store";
import type { AppFormData } from "@/pages/setup/add-app-wizard/app-wizard-validation";
import { isTableSupported, MAX_SELECTED_TABLES } from "@/pages/setup/add-app-wizard/discover-utils";
import { AppWizardAtStep } from "@/pages/setup/add-app-wizard/stories/app-wizard-at-step";
import { replayPlannerTurns } from "@/pages/setup/add-app-wizard/stories/app-wizard-fixtures";

const meta = {
    title: "Setup/Add App Wizard/Verify Schema",
    parameters: {
        page: { bare: true },
    },
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

/**
 * What the select-all box actually draws. `aria-checked` was already correct while a partial
 * selection still rendered the same checkmark as a complete one, so the mark is the part worth
 * asserting: "check" for every row, "dash" for some, "empty" for none.
 */
function readSelectAllMark(canvasElement: HTMLElement): "check" | "dash" | "empty" {
    const box = within(canvasElement).getByRole("checkbox", { name: "Select all" });
    const marks = [...box.querySelectorAll("svg")].filter((svg) => getComputedStyle(svg).display !== "none");

    if (marks.length === 0) {
        return "empty";
    }
    if (marks.length > 1) {
        throw new Error(`Expected one visible mark in the select-all box, found ${marks.length}`);
    }

    return marks[0]!.classList.contains("lucide-minus") ? "dash" : "check";
}

// Verified tables (one with table-level warnings), tables that need configuration (CDC
// disabled and an unsupported reason), and a response-level warning banner.
export const Default: Story = {
    parameters: { msw: { handlers: discoverHandlers(discoveryWithAllStates) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={discoveryWithAllStates} />,
    // Every verified table starts selected, so the header draws a check rather than a partial dash.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await waitFor(() => expect(canvas.getByRole("checkbox", { name: "Select all" })).toBeInTheDocument());
        expect(readSelectAllMark(canvasElement)).toBe("check");
    },
};

const allVerifiedDiscovery: DiscoverResponse = {
    ...discoveryWithAllStates,
    warnings: [],
    tables: discoveryWithAllStates.tables.filter((table) => isTableSupported(discoveryWithAllStates, table)),
};

export const AllVerified: Story = {
    parameters: { msw: { handlers: discoverHandlers(allVerifiedDiscovery) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={allVerifiedDiscovery} />,
    // Without tables that need configuration the tabs are hidden, but the warnings filter still shows.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await waitFor(() => expect(canvas.getAllByRole("checkbox", { name: "Select row" })).toHaveLength(3));
        expect(canvas.queryByRole("tablist")).not.toBeInTheDocument();
        expect(canvas.getByRole("group", { name: "Filter tables by warnings" })).toBeInTheDocument();
    },
};

export const WarningsFilter: Story = {
    parameters: { msw: { handlers: discoverHandlers(discoveryWithAllStates) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={discoveryWithAllStates} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const rowCount = () => canvas.getAllByRole("checkbox", { name: "Select row" }).length;

        await waitFor(() => expect(rowCount()).toBe(3));

        await userEvent.click(canvas.getByRole("radio", { name: /^no warnings/i }));
        await waitFor(() => expect(rowCount()).toBe(2));
        expect(canvas.queryByText("dbo.Orders")).not.toBeInTheDocument();

        await userEvent.click(canvas.getByRole("radio", { name: /^no warnings/i }));
        await waitFor(() => expect(rowCount()).toBe(3));

        await userEvent.click(canvas.getByRole("radio", { name: /^warnings/i }));
        await waitFor(() => expect(rowCount()).toBe(1));
        expect(canvas.getByText("dbo.Orders")).toBeInTheDocument();
        expect(canvas.getByText(/3 out of 5 tables selected/)).toBeInTheDocument();

        await userEvent.type(canvas.getByPlaceholderText("Search by table name..."), "orders");
        expect(canvas.getByRole("radio", { name: /^all\s*1$/i })).toBeInTheDocument();
        expect(canvas.getByRole("radio", { name: /^no warnings\s*0$/i })).toBeInTheDocument();
        expect(canvas.getByRole("radio", { name: /^warnings\s*1$/i })).toBeInTheDocument();
    },
};

// Discovery failed: only the destructive error banner is shown, no tables.
export const DiscoveryFailed: Story = {
    parameters: { msw: { handlers: discoverHandlers(failedDiscovery) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={failedDiscovery} />,
};

// The array-level "at least one table" error must clear as soon as a table is selected.
export const WithoutSelection: Story = {
    render: () => <AppWizardAtStep initialStep="verifySchema" hasSelectedTables={false} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));
        await waitFor(() => expect(canvas.getByText("At least one table is required")).toBeInTheDocument());

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await waitFor(() => expect(canvas.queryByText("At least one table is required")).not.toBeInTheDocument());
    },
};

// Below the selection limit, select-all takes every row.
export const RangeAndSelectAll: Story = {
    parameters: { msw: { handlers: discoverHandlers(manyTablesDiscovery) } },
    render: () => (
        <AppWizardAtStep initialStep="verifySchema" discovery={manyTablesDiscovery} hasSelectedTables={false} />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // One instance for the whole run: the direct userEvent API starts a fresh one per call,
        // which would drop the held shift key before the range click.
        const user = userEvent.setup();
        const rowCheckboxes = () => canvas.getAllByRole("checkbox", { name: "Select row" });
        const selectAll = () => canvas.getByRole("checkbox", { name: "Select all" });
        const limitNotice = () =>
            canvas.queryByText(`One app processes at most ${MAX_SELECTED_TABLES} tables`, { exact: false });
        const previewedRows = () => canvasElement.querySelectorAll(`.${RANGE_PREVIEW_ROW_CLASSNAME}`);
        const selectAllMark = () => readSelectAllMark(canvasElement);

        expect(canvas.getByText(/to select a range of tables/i)).toBeInTheDocument();
        expect(selectAllMark()).toBe("empty");

        await user.click(rowCheckboxes()[0]);

        // Holding shift previews the range from the anchor to the hovered row before taking it.
        await user.keyboard("{Shift>}");
        await user.hover(rowCheckboxes()[3]);
        await waitFor(() => expect(previewedRows()).toHaveLength(4));

        await user.click(rowCheckboxes()[3]);
        await user.keyboard("{/Shift}");
        await waitFor(() => expect(canvas.getByText(/4 out of 80 tables selected/)).toBeInTheDocument());
        await waitFor(() => expect(previewedRows()).toHaveLength(0));
        expect(limitNotice()).not.toBeInTheDocument();
        // 4 of 80 is a partial selection, so the header must not claim everything is selected.
        expect(selectAll()).toHaveAttribute("aria-checked", "mixed");
        expect(selectAllMark()).toBe("dash");

        await user.click(selectAll());
        await waitFor(() => expect(canvas.getByText(/80 out of 80 tables selected/)).toBeInTheDocument());
        expect(limitNotice()).not.toBeInTheDocument();
        expect(selectAllMark()).toBe("check");

        await user.click(selectAll());
        await waitFor(() => expect(selectAllMark()).toBe("empty"));
    },
};

// A shift-click has to be able to start a multi-selection from an empty one. The range used to
// inherit the anchor row's own state, so with nothing selected the click cleared an already empty
// range - and suppressed the plain toggle on the way - leaving the click with no effect at all.
export const ShiftSelectFromEmpty: Story = {
    parameters: { msw: { handlers: discoverHandlers(manyTablesDiscovery) } },
    render: () => (
        <AppWizardAtStep initialStep="verifySchema" discovery={manyTablesDiscovery} hasSelectedTables={false} />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const user = userEvent.setup();
        const rowCheckboxes = () => canvas.getAllByRole("checkbox", { name: "Select row" });
        const selectedCount = () => rowCheckboxes().filter((box) => box.getAttribute("aria-checked") === "true").length;

        // Leaves an anchor behind on an empty selection - where an operator lands after picking a
        // table and changing their mind.
        await user.click(rowCheckboxes()[0]);
        await waitFor(() => expect(selectedCount()).toBe(1));
        await user.click(rowCheckboxes()[0]);
        await waitFor(() => expect(selectedCount()).toBe(0));

        await user.keyboard("{Shift>}");
        await user.click(rowCheckboxes()[4]);
        await user.keyboard("{/Shift}");

        await waitFor(() => expect(canvas.getByText(/5 out of 80 tables selected/)).toBeInTheDocument());

        // Shift-clicking inside the range it just took clears it again.
        await user.keyboard("{Shift>}");
        await user.click(rowCheckboxes()[4]);
        await user.keyboard("{/Shift}");

        await waitFor(() => expect(selectedCount()).toBe(0));
    },
};

// The anchor is only ever recorded by clicking a row, so a selection that arrived any other way -
// seeded from a stored configuration, or cleared through "Deselect all" - used to leave the first
// shift-click with no second endpoint, degrading it to a plain single toggle.
export const ShiftSelectWithoutClicking: Story = {
    parameters: { msw: { handlers: discoverHandlers(discoveryWithAllStates) } },
    render: () => <AppWizardAtStep initialStep="verifySchema" discovery={discoveryWithAllStates} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const user = userEvent.setup();
        const rowCheckboxes = () => canvas.getAllByRole("checkbox", { name: "Select row" });
        const selectedCount = () => rowCheckboxes().filter((box) => box.getAttribute("aria-checked") === "true").length;

        // Clears the seeded selection without ever clicking a row, so no anchor is recorded.
        await user.click(canvas.getByRole("button", { name: /deselect all/i }));
        await waitFor(() => expect(selectedCount()).toBe(0));

        // The range still opens, counted from the top of the table.
        await user.keyboard("{Shift>}");
        await user.click(rowCheckboxes()[2]);
        await user.keyboard("{/Shift}");

        await waitFor(() => expect(selectedCount()).toBe(3));
    },
};

export const CdcVerificationFailed: Story = {
    parameters: {
        msw: { handlers: { setup: [setupMocks.verifyCdc(failedCdcVerification), ...defaultApiMocks.setup] } },
    },
    render: () => <AppWizardAtStep initialStep="verifySchema" />,
    // The dry run's blockers keep the wizard on this step and stay on screen until the selection changes.
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        // The failed run reports an error and a warning, so the alert shows the two-entry summary
        // and keeps the individual blockers in its collapsible details.
        const findAlert = () => canvas.queryByText(/data source verification failed for the selected tables/i);

        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));
        await waitFor(() => expect(findAlert()).toBeInTheDocument());
        expect(canvas.getByRole("heading", { name: /verify your schema/i })).toBeInTheDocument();

        await userEvent.click(canvas.getByRole("button", { name: /show details/i }));
        expect(canvas.getByText(/must have the REPLICATION role attribute/i)).toBeInTheDocument();

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await waitFor(() => expect(findAlert()).not.toBeInTheDocument());
    },
};

const withPlannerSession = (seed: AppFormData) =>
    replayPlannerTurns([{ frames: plannerOpeningFrames }], seed.verifySchema.tables);

// Changing the tables under a planner session asks before throwing the session away.
export const ChangedUnderPlannerSession: Story = {
    render: () => (
        <AppWizardAtStep
            initialStep="verifySchema"
            seedOverride={(seed) => ({ ...seed, mapTables: { tables: [] } })}
            seedStore={withPlannerSession}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));

        await waitFor(() => expect(body.getByRole("alertdialog")).toBeInTheDocument());
        expect(body.getByText("Start the planner over?")).toBeInTheDocument();

        await userEvent.click(body.getByRole("button", { name: /cancel/i }));
        await waitFor(() => expect(body.queryByRole("alertdialog")).not.toBeInTheDocument());
        expect(canvas.getByRole("heading", { name: /verify your schema/i })).toBeInTheDocument();
        expect(useSetupWizardStore.getState().plannerConversationId).toBe(PLANNER_CONVERSATION_ID);

        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));
        await userEvent.click(await body.findByRole("button", { name: /start over/i }));

        await waitFor(() =>
            expect(canvas.getByRole("heading", { name: /plan your document model/i })).toBeInTheDocument(),
        );
        // The planner starts over on the new selection.
        expect(useSetupWizardStore.getState().plannerConversationId).not.toBe(PLANNER_CONVERSATION_ID);
        expect(useSetupWizardStore.getState().plannerSelectedTables).toHaveLength(sampleDiscovery.tables.length - 1);
    },
};

// The planner reads its tables as it opens, so a session still opening counts as one too.
export const ChangedWhilePlannerOpens: Story = {
    tags: ["!dev"],
    render: () => (
        <AppWizardAtStep
            initialStep="verifySchema"
            seedStore={(seed) => replayPlannerTurns([{ frames: [], isWorking: true }], seed.verifySchema.tables)}
        />
    ),
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);
        const body = within(canvasElement.ownerDocument.body);

        await userEvent.click(canvas.getAllByRole("checkbox", { name: "Select row" })[0]);
        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));

        await waitFor(() => expect(body.getByText("Start the planner over?")).toBeInTheDocument());
    },
};

// An unchanged selection moves on without asking.
export const UnchangedUnderPlannerSession: Story = {
    tags: ["!dev"],
    render: () => <AppWizardAtStep initialStep="verifySchema" seedStore={withPlannerSession} />,
    play: async ({ canvasElement }) => {
        const canvas = within(canvasElement);

        await userEvent.click(canvas.getByRole("button", { name: /^next$/i }));

        await waitFor(() =>
            expect(canvas.getByRole("heading", { name: /plan your document model/i })).toBeInTheDocument(),
        );
        expect(useSetupWizardStore.getState().plannerConversationId).toBe(PLANNER_CONVERSATION_ID);
    },
};
