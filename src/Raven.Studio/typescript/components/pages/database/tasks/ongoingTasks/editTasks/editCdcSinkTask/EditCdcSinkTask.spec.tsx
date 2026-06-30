import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask.stories";
import { act, rtlRender, waitFor } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";

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
};

describe("Edit CDC Sink task", () => {
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

    it("closes the test panel without crashing when its table is removed", async () => {
        const Story = composeStory(stories.EditTask, stories.default);

        const { screen, user, fireClick } = rtlRender(<Story />);

        await screen.findByText(selectors.editTaskTitle);

        // Select the configured root table, then open its test panel.
        await fireClick((await screen.findByText("orders")).closest("button"));
        await fireClick((await screen.findByText(/^Test$/)).closest("button"));
        expect(await screen.findByText("Test mapping")).toBeInTheDocument();

        // Remove the table via its actions menu while the test panel is still open.
        await act(async () => {
            await user.click(screen.getByTitle("Table actions"));
        });
        await act(async () => {
            await user.click(await screen.findByText(/^Remove$/));
        });

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
    await act(async () => {
        await user.click(input);
    });
    await act(async () => {
        await user.click(await screen.findByText(option));
    });
}

function getSelectInputByLabel(screen: ReturnType<typeof rtlRender>["screen"], label: string) {
    return screen.getByText(label).closest(".mb-3").querySelector("input");
}

function getButtonByText(screen: ReturnType<typeof rtlRender>["screen"], text: string | RegExp) {
    return screen.getByText(text).closest("button");
}
