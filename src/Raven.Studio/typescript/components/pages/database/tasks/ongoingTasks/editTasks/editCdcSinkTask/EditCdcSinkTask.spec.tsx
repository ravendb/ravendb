import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask.stories";
import { rtlRender, waitFor } from "test/rtlTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";

describe("Edit CDC Sink task", () => {
    it("can render new task view", async () => {
        const Story = composeStory(stories.NewTask, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText("New CDC Sink task")).toBeInTheDocument();
        expect(screen.getByText("Configure basic settings")).toBeInTheDocument();
        expect(screen.getByText("Schema Explorer")).toBeInTheDocument();
        expect(screen.getByText("Configured Tables")).toBeInTheDocument();
        expect(screen.getByRole("button", { name: /Save task configuration/i })).toBeDisabled();
    });

    it("can create new task and send valid DTO on save", async () => {
        const Story = composeStory(stories.NewTask, stories.default);

        const { screen, user } = rtlRender(<Story />);

        await screen.findByText("New CDC Sink task");

        await user.type(screen.getByLabelText("Task Name"), "New CDC Sink");
        await selectOption(user, screen, getSelectInputByLabel(screen, "Connection String"), "sql-name");

        await user.click(screen.getByRole("button", { name: /Discover tables/i }));
        await user.click(await screen.findByRole("button", { name: /^Discover$/i }));
        expect(await screen.findByText("dbo.orders")).toBeInTheDocument();

        await user.click(screen.getByRole("row", { name: /dbo\.orders/i }).querySelector("input[type='checkbox']"));
        await user.click(screen.getByRole("button", { name: /Configure selected tables/i }));
        await user.click(screen.getByRole("button", { name: /Save task configuration/i }));

        await waitFor(() => expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalled());
        expect(mockServices.tasksService.mock.saveCdcSinkTask).toHaveBeenCalledWith("db1", {
            TaskId: null,
            Name: "New CDC Sink",
            Disabled: false,
            ConnectionStringName: "sql-name",
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

        expect(await screen.findByText("Edit CDC Sink task")).toBeInTheDocument();
        expect(await screen.findByDisplayValue("CdcSinkTask")).toBeInTheDocument();
        expect(mockServices.tasksService.mock.getCdcSinkTaskInfo).toHaveBeenCalledWith(
            "db1",
            TasksStubs.getCdcSink().TaskId
        );
    });

    it("can render license restricted view", async () => {
        const Story = composeStory(stories.LicenseRestricted, stories.default);

        const { screen } = rtlRender(<Story />);

        expect(await screen.findByText("New CDC Sink task")).toBeInTheDocument();
        expect(screen.getAllByText("Enterprise").length).toBeGreaterThan(0);
        expect(screen.getByRole("button", { name: /Save task configuration/i })).toBeDisabled();
    });
});

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
