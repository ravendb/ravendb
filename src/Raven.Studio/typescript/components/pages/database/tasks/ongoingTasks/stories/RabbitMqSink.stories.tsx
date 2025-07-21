import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import { userEvent, within } from "@storybook/test";
import React from "react";
import { commonInit } from "components/pages/database/tasks/ongoingTasks/stories/common";
import { Meta, StoryObj } from "@storybook/react";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import OngoingTaskQueueSinkListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink;
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

export default {
    title: "Pages/Tasks/Ongoing tasks/RabbitMQ Sink",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface RabbitMqSinkProps {
    disabled: boolean;
    completed: boolean;
    customizeTask: (x: OngoingTaskQueueSinkListView) => void;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<RabbitMqSinkProps> = {
    render: (args: RabbitMqSinkProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getRabbitSink();
            if (args.disabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            args.customizeTask?.(ongoingTask);
            x.OngoingTasks = [ongoingTask];
            x.PullReplications = [];
            x.SubscriptionsCount = 0;
        };

        tasksService.withGetTasks(mockedValue);

        return <OngoingTasksPage />;
    },
    args: {
        completed: true,
        disabled: false,
        customizeTask: undefined,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
    play: async ({ canvas }) => {
        const container = within(await canvas.findByTestId("sinks"));
        await userEvent.click(await container.findByTitle(/Click for details/));
    },
};

export const Disabled: StoryObj<RabbitMqSinkProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};
