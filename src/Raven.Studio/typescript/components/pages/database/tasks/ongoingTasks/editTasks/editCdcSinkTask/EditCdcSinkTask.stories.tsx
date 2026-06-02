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

export const UnavailableTables: StoryObj = {
    render: () => {
        prepareMocks();
        mockServices.tasksService.withGetCdcSinkTaskInfo();
        mockServices.tasksService.withGetCdcSinkTaskSchema(getSchemaWithDisabledCompanies(false));

        return <EditCdcSinkTask queryParams={{ taskId: String(TasksStubs.getCdcSink().TaskId) }} />;
    },
};

export const AutoProvisioningTables: StoryObj = {
    render: () => {
        prepareMocks();
        mockServices.tasksService.withGetCdcSinkTaskInfo();
        mockServices.tasksService.withGetCdcSinkTaskSchema(getSchemaWithDisabledCompanies(true));

        return <EditCdcSinkTask queryParams={{ taskId: String(TasksStubs.getCdcSink().TaskId) }} />;
    },
};

export const TableWarnings: StoryObj = {
    render: () => {
        prepareMocks();
        mockServices.tasksService.withGetCdcSinkTaskInfo();
        mockServices.tasksService.withGetCdcSinkTaskSchema(getSchemaWithTableWarning());

        return <EditCdcSinkTask queryParams={{ taskId: String(TasksStubs.getCdcSink().TaskId) }} />;
    },
};

function getSchemaWithDisabledCompanies(hasPermissionToSetup: boolean) {
    const schema = TasksStubs.cdcSinkTaskSchema();
    const companies = schema.Tables.find((table) => table.SourceTableName === "companies");

    schema.HasPermissionToSetup = hasPermissionToSetup;
    companies.IsCdcEnabled = false;

    for (const column of companies.Columns) {
        column.IsCdcCapturable = false;
        column.UnsupportedReason = "Table is not enrolled in SQL Server CDC.";
    }

    return schema;
}

function getSchemaWithTableWarning() {
    const schema = TasksStubs.cdcSinkTaskSchema();
    const orders = schema.Tables.find((table) => table.SourceTableName === "orders");

    orders.Warnings = ["REPLICA IDENTITY is set to NOTHING, so DELETE events carry no columns."];

    return schema;
}
