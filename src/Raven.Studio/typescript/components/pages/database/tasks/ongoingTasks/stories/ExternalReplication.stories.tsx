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
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";

interface ExternalReplicationProps {
    isDisabled: boolean;
    isCompleted: boolean;
    isServerWide: boolean;
    hasRuntimeError: boolean;
    hasLoadError: boolean;
    hasLongValues: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export default {
    title: "Pages/Tasks/Ongoing tasks/External Replication",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args: ExternalReplicationProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;
        const mockedValue: MockedValue<OngoingTasksResult> = (x: OngoingTasksResult) => {
            const ongoingTask = TasksStubs.getExternalReplicationListItem();
            if (args.isDisabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.hasRuntimeError) {
                ongoingTask.Error = "This is some error";
            }
            if (args.isServerWide) {
                ongoingTask.TaskName = "Server Wide External Replication, ext1";
            }
            if (args.hasLongValues) {
                ongoingTask.TaskName =
                    "External Replication of all customer orders to the disaster recovery data center";
                ongoingTask.DestinationDatabase = "customer-orders-disaster-recovery-replica-eu-west-2026";
                ongoingTask.Error =
                    "System.Net.Http.HttpRequestException: No connection could be made because the target machine actively refused it. (dr-replica-eu-west.example.com:8080)";
            }
            x.OngoingTasks = [ongoingTask];
            x.PullReplications = [];
            x.SubscriptionsCount = 0;
        };

        if (args.hasLoadError) {
            tasksService.withThrowingGetTasks((db, location) => location.nodeTag === "C", mockedValue);
        } else {
            tasksService.withGetTasks(mockedValue);
        }

        mockExternalReplicationProgress(tasksService, args.isCompleted);

        return <OngoingTasksPage />;
    },
    args: {
        isCompleted: true,
        isDisabled: false,
        isServerWide: false,
        hasRuntimeError: false,
        hasLoadError: false,
        hasLongValues: false,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
    play: async ({ canvas }) => {
        const container = within(await canvas.findByTestId("replications"));
        await userEvent.click(await container.findByTitle(/Click for details/));
    },
} satisfies Meta<ExternalReplicationProps>;

export const Default: StoryObj<ExternalReplicationProps> = {};

export const NonSharded: StoryObj<ExternalReplicationProps> = {
    args: {
        databaseType: "cluster",
    },
};

export const Disabled: StoryObj<ExternalReplicationProps> = {
    args: {
        isDisabled: true,
    },
};

export const LoadError: StoryObj<ExternalReplicationProps> = {
    args: {
        hasLoadError: true,
    },
};

export const RuntimeError: StoryObj<ExternalReplicationProps> = {
    args: {
        hasRuntimeError: true,
    },
};

export const ServerWide: StoryObj<ExternalReplicationProps> = {
    args: {
        isServerWide: true,
    },
};

export const LongValues: StoryObj<ExternalReplicationProps> = {
    args: {
        hasLongValues: true,
    },
};
