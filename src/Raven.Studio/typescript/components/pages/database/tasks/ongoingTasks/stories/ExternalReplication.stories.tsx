import { Meta, StoryObj } from "@storybook/react-webpack5";
import { mockServices } from "test/mocks/services/MockServices";
import { MockedValue } from "test/mocks/services/AutoMockService";
import { TasksStubs } from "test/stubs/TasksStubs";
import { userEvent, within } from "storybook/test";
import React from "react";
import {
    commonInit,
    mockExternalReplicationProgress,
} from "components/pages/database/tasks/ongoingTasks/stories/common";
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";

export default {
    title: "Pages/Tasks/Ongoing tasks/External Replication",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface ExternalReplicationProps {
    disabled: boolean;
    completed: boolean;
    customizeTask: (x: OngoingTaskReplication) => void;
    runtimeError: boolean;
    loadError: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<ExternalReplicationProps> = {
    render: (args: ExternalReplicationProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;
        const mockedValue: MockedValue<OngoingTasksResult> = (x: OngoingTasksResult) => {
            const ongoingTask = TasksStubs.getExternalReplicationListItem();
            if (args.disabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.runtimeError) {
                ongoingTask.Error = "This is some error";
            }
            args.customizeTask?.(ongoingTask);
            x.OngoingTasks = [ongoingTask];
            x.PullReplications = [];
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

export const NonSharded: StoryObj<ExternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        databaseType: "cluster",
    },
};

export const Disabled: StoryObj<ExternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const LoadError: StoryObj<ExternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        loadError: true,
    },
};

export const RuntimeError: StoryObj<ExternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        runtimeError: true,
    },
};

export const ServerWide: StoryObj<ExternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: false,
        customizeTask: (task) => {
            task.TaskName = "Server Wide External Replication, ext1";
        },
    },
};
