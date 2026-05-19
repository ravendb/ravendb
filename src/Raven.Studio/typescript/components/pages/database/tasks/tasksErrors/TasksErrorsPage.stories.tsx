import {
    databaseAccessArgType,
    databaseArgType,
    DatabaseType,
    withBootstrap5,
    withForceRerender,
    withStorybookContexts,
} from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import TasksErrorsPage from "components/pages/database/tasks/tasksErrors/TasksErrorsPage";
import { mockStore } from "test/mocks/store/MockStore";
import { mockServices } from "test/mocks/services/MockServices";
import assertUnreachable from "components/utils/assertUnreachable";
import { TasksStubs } from "test/stubs/TasksStubs";

export default {
    title: "Pages/Tasks/Task Errors",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/zOjJhPl1Jk1McZXXBwcw10/Pages---Tasks-Errors?node-id=639-19865&t=RKPuQ28LNLPaRFpl-0",
        },
    },
} satisfies Meta;

interface TasksErrorsPageArgs {
    hasErrors: boolean;
    databaseType: DatabaseType;
    databaseAccess: databaseAccessLevel;
}

export const Default: StoryObj<TasksErrorsPageArgs> = {
    name: "Task Errors",
    render: ({ hasErrors, databaseType, databaseAccess }) => {
        const { databases, accessManager } = mockStore;
        const { tasksService } = mockServices;

        let db = null;

        switch (databaseType) {
            case "sharded":
                db = databases.withActiveDatabase_Sharded();
                break;
            case "cluster":
                db = databases.withActiveDatabase_NonSharded_Cluster();
                break;
            case "singleNode":
                db = databases.withActiveDatabase_NonSharded_SingleNode();
                break;
            default:
                assertUnreachable(databaseType);
        }

        accessManager.with_databaseAccess({
            [db.name]: databaseAccess,
        });

        tasksService.withEtlErrors(hasErrors ? TasksStubs.etlErrors() : []);

        const etlStats = hasErrors ? TasksStubs.etlStats() : [];
        if (databaseType === "sharded") {
            etlStats.forEach((stat) => (stat.ShardNumber = 0));
        }
        tasksService.withEtlStats(etlStats);
        return <TasksErrorsPage />;
    },
    argTypes: {
        databaseType: databaseArgType,
        databaseAccess: databaseAccessArgType,
    },
    args: {
        hasErrors: true,
        databaseType: "singleNode",
        databaseAccess: "DatabaseAdmin",
    },
};
