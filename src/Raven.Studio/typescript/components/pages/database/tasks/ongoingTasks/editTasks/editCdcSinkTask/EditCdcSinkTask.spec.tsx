import { composeStory } from "@storybook/react-webpack5";
import * as stories from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask.stories";
import { rtlRender } from "test/rtlTestUtils";
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
