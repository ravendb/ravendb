import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import React from "react";
import {
    commonInit,
    mockExternalReplicationProgress,
} from "components/pages/database/tasks/ongoingTasks/stories/common";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react";
import { userEvent, within } from "@storybook/test";
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

export default {
    title: "Pages/Tasks/Ongoing tasks/Replication Hub",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface ReplicationHubProps {
    disabled: boolean;
    completed: boolean;
    withOutConnections: boolean;
    customizeTask: (x: OngoingTaskPullReplicationAsHub) => void;
    emptyScript: boolean;
    runtimeError: boolean;
    loadError: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<ReplicationHubProps> = {
    render: (args: ReplicationHubProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getReplicationHub();
            if (args.disabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.runtimeError) {
                ongoingTask.Error = "This is some error";
            }
            x.PullReplications.forEach((definition) => {
                definition.Disabled = args.disabled;
            });
            args.customizeTask?.(ongoingTask);
            x.OngoingTasks = args.withOutConnections ? [] : [ongoingTask];
            x.PullReplications = x.PullReplications.filter((x) =>
                args.withOutConnections ? x.Name === "EmptyHub" : x.Name !== "EmptyHub"
            );
            x.SubscriptionsCount = 0;
        };

        if (args.loadError) {
            tasksService.withThrowingGetTasks((db, location) => location.nodeTag === "C", mockedValue);
        } else {
            tasksService.withGetTasks(mockedValue);
        }

        mockExternalReplicationProgress(tasksService, args.completed);

        return <OngoingTasksPage />;
    },
    args: {
        completed: true,
        disabled: false,
        runtimeError: false,
        loadError: false,
        emptyScript: false,
        customizeTask: undefined,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
    play: async ({ canvas }) => {
        const container = within(await canvas.findByTestId("replications"));
        await userEvent.click(await container.findByTitle(/Click for details/));
    },
};

export const Disabled: StoryObj<ReplicationHubProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const NoConnections: StoryObj<ReplicationHubProps> = {
    ...Default,
    args: {
        ...Default.args,
        withOutConnections: true,
    },
};
