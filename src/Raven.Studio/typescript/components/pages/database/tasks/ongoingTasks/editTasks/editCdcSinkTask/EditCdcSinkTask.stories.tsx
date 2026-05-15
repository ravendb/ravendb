import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import EditCdcSinkTask from "components/pages/database/tasks/ongoingTasks/editTasks/editCdcSinkTask/EditCdcSinkTask";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { TasksStubs } from "test/stubs/TasksStubs";

export default {
    title: "Pages/Tasks/Ongoing Tasks/Edit tasks/CDC Sink",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

function prepareMocks(hasCdcSink = true) {
    const { cluster, license, databases } = mockStore;
    const { tasksService } = mockServices;

    databases.withActiveDatabase();
    cluster.with_Single();
    license.with_License({
        HasCdcSink: hasCdcSink,
    });

    tasksService.withConnectionStrings();
    tasksService.withVerifyCdcSink();
    tasksService.withTestCdcSink();
    tasksService.withGetCdcSinkTaskSchema();
}

export const NewTask: StoryObj = {
    render: () => {
        prepareMocks();

        return <EditCdcSinkTask />;
    },
};

export const EditTask: StoryObj = {
    render: () => {
        prepareMocks();
        mockServices.tasksService.withGetCdcSinkTaskInfo();

        return <EditCdcSinkTask queryParams={{ taskId: String(TasksStubs.getCdcSink().TaskId) }} />;
    },
};

export const LicenseRestricted: StoryObj = {
    render: () => {
        prepareMocks(false);

        return <EditCdcSinkTask />;
    },
};
