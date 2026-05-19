import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import React from "react";
import { commonInit, mockEtlProgress } from "components/pages/database/tasks/ongoingTasks/stories/common";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl;
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { userEvent, within } from "storybook/test";

export default {
    title: "Pages/Tasks/Ongoing tasks/RabbitMQ",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface RabbitMqProps {
    disabled: boolean;
    completed: boolean;
    customizeTask: (x: OngoingTaskQueueEtlListView) => void;
    emptyScript: boolean;
    runtimeError: boolean;
    loadError: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<RabbitMqProps> = {
    render: (args: RabbitMqProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getRabbitEtl();
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
        tasksService.withEtlErrors([]);
        tasksService.withEtlStats([]);
        mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

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
        const container = within(await canvas.findByTestId("etls"));
        await userEvent.click(await container.findByTitle(/Click for details/));
    },
};

export const NotSharded: StoryObj<RabbitMqProps> = {
    ...Default,
    args: {
        ...Default.args,
        databaseType: "cluster",
    },
};

export const Disabled: StoryObj<RabbitMqProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const LoadError: StoryObj<RabbitMqProps> = {
    ...Default,
    args: {
        ...Default.args,
        loadError: true,
    },
};

export const RuntimeError: StoryObj<RabbitMqProps> = {
    ...Default,
    args: {
        ...Default.args,
        runtimeError: true,
    },
};

export const EmptyScript: StoryObj<RabbitMqProps> = {
    ...Default,
    args: {
        ...Default.args,
        completed: true,
        emptyScript: true,
    },
};
