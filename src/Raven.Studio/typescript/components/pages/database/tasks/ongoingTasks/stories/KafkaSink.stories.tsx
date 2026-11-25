import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import { userEvent, within } from "storybook/test";
import React from "react";
import { commonInit } from "components/pages/database/tasks/ongoingTasks/stories/common";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

interface KafkaSinkProps {
    isDisabled: boolean;
    isReconnecting: boolean;
    isNotStarted: boolean;
    hasLongValues: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

const LONG_TOPICS = [
    "customer-orders-created-events-eu-west-production",
    "customer-orders-cancelled-events-eu-west-production",
    "customer-payments-settled-events-eu-west-production",
];

export default {
    title: "Pages/Tasks/Ongoing tasks/Kafka Sink",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args: KafkaSinkProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getKafkaSink();
            if (args.isDisabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.isReconnecting) {
                ongoingTask.TaskConnectionStatus = "Reconnect";
            }
            if (args.isNotStarted) {
                ongoingTask.TaskConnectionStatus = "None";
                ongoingTask.Error = `Queue Sink process '${ongoingTask.TaskName}' was not found.`;
            }
            if (args.hasLongValues) {
                ongoingTask.TaskName =
                    "Kafka Sink consuming customer order and payment events from the EU production cluster";
                ongoingTask.ConnectionStringName = "kafka-eu-west-production-cluster-with-sasl-ssl-authentication";
                ongoingTask.Configuration.Scripts[0].Queues = LONG_TOPICS;
            }
            x.OngoingTasks = [ongoingTask];
            x.PullReplications = [];
            x.SubscriptionsCount = 0;
        };

        tasksService.withGetTasks(mockedValue);

        return <OngoingTasksPage />;
    },
    args: {
        isDisabled: false,
        isReconnecting: false,
        isNotStarted: false,
        hasLongValues: false,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
} satisfies Meta<KafkaSinkProps>;

const expandTaskDetails: StoryObj<KafkaSinkProps>["play"] = async ({ canvas }) => {
    const container = within(await canvas.findByTestId("sinks"));
    await userEvent.click(await container.findByTitle(/Click for details/));
};

export const Default: StoryObj<KafkaSinkProps> = {
    play: expandTaskDetails,
};

export const Disabled: StoryObj<KafkaSinkProps> = {
    args: {
        isDisabled: true,
    },
    play: expandTaskDetails,
};

export const Reconnecting: StoryObj<KafkaSinkProps> = {
    args: {
        isReconnecting: true,
    },
};

export const NotStarted: StoryObj<KafkaSinkProps> = {
    args: {
        isNotStarted: true,
    },
};

export const LongValues: StoryObj<KafkaSinkProps> = {
    args: {
        hasLongValues: true,
    },
};
