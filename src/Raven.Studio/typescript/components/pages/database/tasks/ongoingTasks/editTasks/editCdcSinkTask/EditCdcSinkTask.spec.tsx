import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask.stories";
import { act, rtlRender, waitFor, within } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { Ace } from "ace-builds";

const selectors = {
    newTaskTitle: "New CDC Sink task",
    editTaskTitle: "Edit CDC Sink task",
    taskName: "Task Name",
    taskNameValue: "New CDC Sink",
    existingTaskName: "CdcSinkTask",
    connectionString: "Connection String",
    connectionStringValue: "sql-name",
    databaseName: "db1",
    configureBasicSettings: "Configure basic settings",
    schemaExplorer: "Schema Explorer",
    configuredTables: "Configured Tables",
    enterprise: "Enterprise",
    ordersTable: "dbo.orders",
    companiesTable: "dbo.companies",
    availableTables: "Available tables",
    unavailableTables: "Unavailable tables",
    cdcSetupRequired: "CDC setup required",
    cdcSetupRequiredMessage: "CDC is not enabled. Ask a database administrator to enable CDC for this table.",
    tableWarnings: "Table warnings",
    tableWarningMessage: "REPLICA IDENTITY is set to NOTHING, so DELETE events carry no columns.",
    discoverTablesButton: /^Discover tables$/i,
    discoverButton: /^Discover$/i,
    configureSelectedTablesButton: /^Configure selected tables$/i,
    saveTaskButton: /Save task configuration/i,
    missingRelatedTablesAlert: /Linked tables reference 1 source table that is not configured as a root table/,
    addMissingRootTableButton: /^Add root table$/,
    addSelectedMissingRootTablesButton: /^Add 1 root table$/,
    addRootTablesModalTitle: "Add root tables",
    verifyTablesButton: /^Verify tables$/,
    tablesVerifiedButton: /^Tables verified$/,
    verificationFailedButton: /^Verification failed$/,
    verificationFailedTitle: "Data source verification failed for the configured tables.",
    verificationErrorMessage:
        "The database user must have the REPLICATION role attribute to create a replication slot.",
    verificationPassedWithWarningsTitle: "Data source verification passed with warnings.",
    verificationRequestFailedMessage: "The dry run request failed. Check the Notification Center for details.",
    verificationErrorDetails: "EnsureReplicationSlotAsync",
    verificationWarning: "Source cleanup failed: publication rvn_cdc_p_8f3a was left in place.",
    showDetailsButton: /^Show details$/,
    saveAnywayTitle: "Save the task configuration anyway?",
    saveAnywayButton: /^Save anyway$/,
    cancelButton: /^Cancel$/,
    rawConfigSwitch: "Raw config",
    tableActions: "Table actions",
    disableTableAction: /^Disable$/,
};

describe("Edit CDC Sink task", () => {
    beforeEach(() => {
        jest.clearAllMocks();
    });

    it("can render new task view", async () => {
        const Story = composeStory(stories.NewTask, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText(selectors.newTaskTitle)).toBeInTheDocument();
        expect(screen.getByText(selectors.configureBasicSettings)).toBeInTheDocument();
        expect(screen.getByText(selectors.schemaExplorer)).toBeInTheDocument();
        expect(screen.getByText(selectors.configuredTables)).toBeInTheDocument();
        expect(screen.getByRole("button", { name: selectors.saveTaskButton })).toBeDisabled();
    });

    it("can create new task and send valid DTO on save", async () => {
        const Story = composeStory(stories.NewTask, stories.default);

        const { screen, user, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.newTaskTitle);

        await fillInput(screen.getByLabelText(selectors.taskName), selectors.taskNameValue);
        await discoverTables(screen, user, fireClick);
        expect(await screen.findByText(selectors.ordersTable)).toBeInTheDocument();

        await fireClick(screen.getByText(selectors.ordersTable).closest("tr").querySelector("input[type='checkbox']"));
        await fireClick(getButtonByText(screen, selectors.configureSelectedTablesButton));
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalledWith(selectors.databaseName, {
            TaskId: null,
            Name: selectors.taskNameValue,
            Disabled: false,
            ConnectionStringName: selectors.connectionStringValue,
            MentorNode: null,
            PinToMentorNode: false,
            Postgres: null,
            SkipInitialLoad: false,
            Tables: [
                {
                    CollectionName: "Orders",
                    SourceTableName: "orders",
                    SourceTableSchema: "dbo",
                    Disabled: false,
                    PrimaryKeyColumns: ["Id"],
                    Columns: [
                        {
                            Column: "Id",
                            Name: "Id",
                            Type: "Default",
                        },
                        {
                            Column: "CompanyId",
                            Name: "CompanyId",
                            Type: "Default",
                        },
                    ],
                    EmbeddedTables: [],
                    LinkedTables: [
                        {
                            SourceTableName: "companies",
                            SourceTableSchema: "dbo",
                            PropertyName: "CompanyId",
                            LinkedCollectionName: "Companies",
                            JoinColumns: ["CompanyId"],
                        },
                    ],
                    OnDelete: {
                        IgnoreDeletes: false,
                        Patch: null,
                    },
                    Patch: null,
                },
            ],
        });
    });

    it("can render edit task view with existing configuration", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText(selectors.editTaskTitle)).toBeInTheDocument();
        expect(await screen.findByDisplayValue(selectors.existingTaskName)).toBeInTheDocument();
        expect(mockServices.tasksService.mock.getCdcSinkTaskInfo).toHaveBeenCalledWith(
            selectors.databaseName,
            TasksStubs.getCdcSink().TaskId
        );
    });

    it("adds missing referenced tables as root tables from the alert", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        expect(await screen.findByText("orders")).toBeInTheDocument();

        // No "alert is absent before discovery" assertion: discovery also auto-runs for the
        // saved connection string, so such a check would race it.
        await fireClick(getButtonByText(screen, selectors.discoverTablesButton));
        await fireClick(await screen.findByText(selectors.discoverButton).then((x) => x.closest("button")));

        const missingRelatedTablesAlert = await screen.findByText(selectors.missingRelatedTablesAlert);
        const missingRelatedTablesAlertElement = missingRelatedTablesAlert.closest(".alert") as HTMLElement;
        expect(missingRelatedTablesAlert).toBeInTheDocument();
        expect(missingRelatedTablesAlertElement).not.toHaveTextContent(selectors.companiesTable);

        await fireClick(getButtonByText(screen, selectors.addMissingRootTableButton));
        const addRootTablesModal = (await screen.findByText(selectors.addRootTablesModalTitle)).closest(
            ".modal-content"
        ) as HTMLElement;
        expect(within(addRootTablesModal).getByText(selectors.companiesTable)).toBeInTheDocument();

        await fireClick(
            within(addRootTablesModal).getByText(selectors.addSelectedMissingRootTablesButton).closest("button")
        );

        await waitFor(() => expect(screen.queryByText(selectors.missingRelatedTablesAlert)).not.toBeInTheDocument());
        expect(await screen.findByText("companies")).toBeInTheDocument();
    });

    it("can render license restricted view", async () => {
        const Story = composeStory(stories.LicenseRestricted, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText(selectors.newTaskTitle)).toBeInTheDocument();
        expect(screen.getAllByText(selectors.enterprise).length).toBeGreaterThan(0);
        expect(screen.getByRole("button", { name: selectors.saveTaskButton })).toBeDisabled();
    });

    it("shows unavailable tables separately without selection checkboxes", async () => {
        const Story = composeStory(stories.UnavailableTables, stories.default);

        const { screen, user } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);

        expect(await screen.findByText(selectors.availableTables)).toBeInTheDocument();
        expect(screen.getByText(selectors.unavailableTables)).toBeInTheDocument();

        const unavailableTableRow = screen.getByText(selectors.ordersTable).closest("tr");
        expect(unavailableTableRow.querySelector("input[type='checkbox']")).not.toBeInTheDocument();

        const errorIcon = screen.getByLabelText(selectors.cdcSetupRequired);
        await user.hover(errorIcon.closest("div"));

        expect(await screen.findByText(selectors.cdcSetupRequiredMessage)).toBeInTheDocument();
    });

    it("does not show a warning for a table that will be provisioned automatically", async () => {
        const Story = composeStory(stories.AutoProvisioningTables, stories.default);

        const { screen } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);

        const autoProvisionedTableRow = await screen.findByText(selectors.companiesTable).then((x) => x.closest("tr"));
        expect(autoProvisionedTableRow.querySelector("input[type='checkbox']")).toBeInTheDocument();
        expect(screen.queryByText(selectors.cdcSetupRequiredMessage)).not.toBeInTheDocument();
    });

    it("shows table-scoped warnings in the available table row tooltip", async () => {
        const Story = composeStory(stories.TableWarnings, stories.default);

        const { screen, user } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);

        expect(screen.queryByText(selectors.tableWarningMessage)).not.toBeInTheDocument();

        const warningIcon = await screen.findByLabelText(selectors.tableWarnings);
        await user.hover(warningIcon.closest("div"));

        expect(await screen.findByText(selectors.tableWarningMessage)).toBeInTheDocument();
    });

    it("verifies the configured tables against the dry run endpoint with the inline connection", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));

        expect(await screen.findByText(selectors.tablesVerifiedButton)).toBeInTheDocument();

        const verifyMock = jest.mocked(mockServices.tasksService.mock.verifyCdcSink);
        expect(verifyMock).toHaveBeenCalledTimes(1);

        const [verifiedDatabaseName, request] = verifyMock.mock.calls[0];
        expect(verifiedDatabaseName).toBe(selectors.databaseName);
        expect(request.Connection).toEqual({
            Type: "Sql",
            Name: selectors.connectionStringValue,
            FactoryName: expect.any(String),
            ConnectionString: expect.any(String),
        });
        expect(request.Configuration.ConnectionStringName).toBe(selectors.connectionStringValue);
        expect(request.Configuration.Tables[0].SourceTableName).toBe("orders");
        expect(mockServices.tasksService.mock.saveCdcSinkTask).not.toHaveBeenCalled();

        await fireClick(getButtonByText(screen, selectors.tablesVerifiedButton));

        await waitFor(() => expect(verifyMock).toHaveBeenCalledTimes(2));
    });

    it("shows the dry run failure with its details and warnings", async () => {
        const Story = composeStory(stories.VerificationFailed, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));

        expect(await screen.findByText(selectors.verificationFailedTitle)).toBeInTheDocument();
        expect(screen.getByText(selectors.verificationErrorMessage)).toBeInTheDocument();
        expect(screen.getByText(selectors.verificationWarning)).toBeInTheDocument();
        expect(screen.queryByText(selectors.verificationErrorDetails, { exact: false })).not.toBeInTheDocument();

        await fireClick(getButtonByText(screen, selectors.showDetailsButton));
        expect(screen.getByText(selectors.verificationErrorDetails, { exact: false })).toBeInTheDocument();

        expect(getButtonByText(screen, selectors.verificationFailedButton)).toBeEnabled();
    });

    it("keeps the dry run warnings visible after a passing run", async () => {
        const Story = composeStory(stories.VerificationPassedWithWarnings, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));

        expect(await screen.findByText(selectors.verificationPassedWithWarningsTitle)).toBeInTheDocument();
        expect(screen.getByText(selectors.verificationWarning)).toBeInTheDocument();
        expect(screen.queryByText(selectors.verificationFailedTitle)).not.toBeInTheDocument();
    });

    it("resets the verification result when the verified inputs change", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, user, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));
        expect(await screen.findByText(selectors.tablesVerifiedButton)).toBeInTheDocument();

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        expect(getButtonByText(screen, selectors.tablesVerifiedButton)).toBeInTheDocument();

        await fireClick((await screen.findByText("orders")).closest("button"));
        await user.click(screen.getByTitle(selectors.tableActions));
        await user.click(await screen.findByText(selectors.disableTableAction));

        expect(await screen.findByText(selectors.verifyTablesButton)).toBeInTheDocument();
    });

    it("runs the dry run before saving and saves when it passes", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());

        const verifyMock = mockServices.tasksService.mock.verifyCdcSink;
        expect(verifyMock).toHaveBeenCalledTimes(1);
        expect(verifyMock).toHaveBeenCalledBefore(jest.mocked(mockServices.tasksService.mock.saveCdcSinkTask));
        expect(screen.queryByText(selectors.saveAnywayTitle)).not.toBeInTheDocument();
    });

    it("reuses the current verification result when saving", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));
        expect(await screen.findByText(selectors.tablesVerifiedButton)).toBeInTheDocument();

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.verifyCdcSink).toHaveBeenCalledTimes(1);
    });

    it("asks for confirmation before saving when the dry run fails", async () => {
        const Story = composeStory(stories.VerificationFailed, stories.default);

        const { screen, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        const dialog = await screen.findByRole("dialog");
        expect(within(dialog).getByText(selectors.saveAnywayTitle)).toBeInTheDocument();
        expect(within(dialog).getByText(selectors.verificationFailedTitle)).toBeInTheDocument();
        expect(within(dialog).getByText(selectors.verificationErrorMessage)).toBeInTheDocument();
        expect(within(dialog).getByText(selectors.verificationWarning)).toBeInTheDocument();

        await fireClick(within(dialog).getByRole("button", { name: selectors.cancelButton }));

        await waitFor(() => expect(screen.queryByRole("dialog")).not.toBeInTheDocument());
        expect(mockServices.tasksService.mock.saveCdcSinkTask).not.toHaveBeenCalled();
        expect(getButtonByText(screen, selectors.verificationFailedButton)).toBeInTheDocument();

        await fireClick(getButtonByText(screen, selectors.saveTaskButton));
        await fireClick(
            within(await screen.findByRole("dialog")).getByRole("button", { name: selectors.saveAnywayButton })
        );

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.verifyCdcSink).toHaveBeenCalledTimes(1);
    });

    it("asks for confirmation before saving when the dry run passes with warnings", async () => {
        const Story = composeStory(stories.VerificationPassedWithWarnings, stories.default);

        const { screen, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        const dialog = await screen.findByRole("dialog");
        expect(within(dialog).getByText(selectors.saveAnywayTitle)).toBeInTheDocument();
        expect(within(dialog).getByText(selectors.verificationPassedWithWarningsTitle)).toBeInTheDocument();
        expect(within(dialog).getByText(selectors.verificationWarning)).toBeInTheDocument();

        await fireClick(within(dialog).getByRole("button", { name: selectors.saveAnywayButton }));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
    });

    it("offers to save anyway when the dry run request fails", async () => {
        const Story = composeStory(stories.VerificationRequestFailed, stories.default);

        const { screen, fillInput, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(getButtonByText(screen, selectors.verifyTablesButton));
        expect(await screen.findByText(selectors.verificationFailedButton)).toBeInTheDocument();
        expect(screen.getByText(selectors.verificationRequestFailedMessage)).toBeInTheDocument();

        await fillInput(screen.getByLabelText(selectors.taskName), "Renamed task");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        const dialog = await screen.findByRole("dialog");
        expect(within(dialog).getByText(selectors.verificationRequestFailedMessage)).toBeInTheDocument();

        await fireClick(within(dialog).getByRole("button", { name: selectors.saveAnywayButton }));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.verifyCdcSink).toHaveBeenCalledTimes(1);
    });

    it("saves the edited raw configuration", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(screen.getByLabelText(selectors.rawConfigSwitch));
        expect(getButtonByText(screen, selectors.saveTaskButton)).toBeDisabled();

        await updateRawConfig((config) => ({ ...config, Name: "Raw task" }));
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalledWith(
            selectors.databaseName,
            expect.objectContaining({ Name: "Raw task" })
        );
    });

    it("does not save a raw configuration with invalid JSON", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(screen.getByLabelText(selectors.rawConfigSwitch));
        await setRawConfig("{");
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        expect(screen.getByLabelText(selectors.rawConfigSwitch)).toBeChecked();
        expect(mockServices.tasksService.mock.verifyCdcSink).not.toHaveBeenCalled();
        expect(mockServices.tasksService.mock.saveCdcSinkTask).not.toHaveBeenCalled();
    });

    it("switches to the form view when the raw configuration has validation errors", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);
        await screen.findByText(selectors.ordersTable);

        await fireClick(screen.getByLabelText(selectors.rawConfigSwitch));
        await updateRawConfig((config) => ({ ...config, Name: "" }));
        await fireClick(getButtonByText(screen, selectors.saveTaskButton));

        await waitFor(() => expect(screen.getByLabelText(selectors.rawConfigSwitch)).not.toBeChecked());
        expect(screen.getByLabelText(selectors.taskName)).toHaveValue("");
        expect(mockServices.tasksService.mock.verifyCdcSink).not.toHaveBeenCalled();
        expect(mockServices.tasksService.mock.saveCdcSinkTask).not.toHaveBeenCalled();
    });

    it("closes the test panel without crashing when its table is removed", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, user, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);

        // Select the configured root table, then open its test panel.
        await fireClick((await screen.findByText("orders")).closest("button"));
        await fireClick((await screen.findByText(/^Test$/)).closest("button"));
        expect(await screen.findByText("Test mapping")).toBeInTheDocument();

        // Remove the table via its actions menu while the test panel is still open.
        await user.click(screen.getByTitle("Table actions"));
        await user.click(await screen.findByText(/^Remove$/));

        // The page must not crash, and the orphaned test panel must be closed.
        await waitFor(() => expect(screen.queryByText("Test mapping")).not.toBeInTheDocument());
        expect(screen.getByText(selectors.editTaskTitle)).toBeInTheDocument();
    });
});

async function discoverTables(
    screen: ReturnType<typeof rtlRender>["screen"],
    user: ReturnType<typeof rtlRender>["user"],
    fireClick: ReturnType<typeof rtlRender>["fireClick"]
) {
    await selectOption(
        user,
        screen,
        getSelectInputByLabel(screen, selectors.connectionString),
        selectors.connectionStringValue
    );

    await fireClick(getButtonByText(screen, selectors.discoverTablesButton));
    await fireClick(await screen.findByText(selectors.discoverButton).then((x) => x.closest("button")));
}

async function selectOption(
    user: ReturnType<typeof rtlRender>["user"],
    screen: ReturnType<typeof rtlRender>["screen"],
    input: HTMLElement,
    option: string
) {
    await user.click(input);
    await user.click(await screen.findByText(option));
}

function getSelectInputByLabel(screen: ReturnType<typeof rtlRender>["screen"], label: string) {
    return screen.getByText(label).closest(".mb-3").querySelector("input");
}

function getRawConfigEditor(): Ace.Editor {
    return (document.querySelector(".ace_editor") as HTMLElement & { env: { editor: Ace.Editor } }).env.editor;
}

async function setRawConfig(content: string) {
    await act(async () => {
        getRawConfigEditor().setValue(content);
    });
}

async function updateRawConfig(
    update: (config: Raven.Client.Documents.Operations.CdcSink.CdcSinkConfiguration) => object
) {
    await setRawConfig(JSON.stringify(update(JSON.parse(getRawConfigEditor().getValue()))));
}

function getButtonByText(screen: ReturnType<typeof rtlRender>["screen"], text: string | RegExp) {
    return screen.getByText(text).closest("button");
}
