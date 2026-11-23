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

interface CdcSinkProps {
    isDisabled: boolean;
    hasRuntimeError: boolean;
    isReconnecting: boolean;
    hasHealthIssue: boolean;
    hasLongValues: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

const SECONDS_SINCE_LAST_ACTIVITY = 142;

export default {
    title: "Pages/Tasks/Ongoing tasks/CDC Sink",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args: CdcSinkProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getCdcSink();
            ongoingTask.LastActivityTime = new Date(Date.now() - SECONDS_SINCE_LAST_ACTIVITY * 1000).toISOString();
            ongoingTask.SecondsSinceLastActivity = SECONDS_SINCE_LAST_ACTIVITY;

            if (args.isDisabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.hasRuntimeError) {
                ongoingTask.Error = "CDC Sink process 'CdcSinkTask' was not found.";
            }
            if (args.isReconnecting) {
                ongoingTask.TaskConnectionStatus = "Reconnect";
                ongoingTask.HealthIssue =
                    "Process is in error recovery (fallback mode). Last error: 2026-11-23T08:34:44.7081752Z. Next retry in: 971s.";
            }
            if (args.hasHealthIssue) {
                ongoingTask.HealthIssue = `No binlog activity for ${SECONDS_SINCE_LAST_ACTIVITY}s (expected heartbeat every 30s). The MySQL binlog connection may be dead.`;
            }
            if (args.hasLongValues) {
                ongoingTask.TaskName =
                    "CDC Sink importing customer orders and invoices from the EU production PostgreSQL";
                ongoingTask.ConnectionStringName = "postgres-eu-west-production-orders-cluster-with-ssl-authentication";
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
        hasRuntimeError: false,
        isReconnecting: false,
        hasHealthIssue: false,
        hasLongValues: false,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
} satisfies Meta<CdcSinkProps>;

const expandTaskDetails: StoryObj<CdcSinkProps>["play"] = async ({ canvas }) => {
    const container = within(await canvas.findByTestId("sinks"));
    await userEvent.click(await container.findByTitle(/Click for details/));
};

export const Default: StoryObj<CdcSinkProps> = {
    play: expandTaskDetails,
};

export const Disabled: StoryObj<CdcSinkProps> = {
    args: {
        isDisabled: true,
    },
    play: expandTaskDetails,
};

export const RuntimeError: StoryObj<CdcSinkProps> = {
    args: {
        hasRuntimeError: true,
    },
};

export const Reconnecting: StoryObj<CdcSinkProps> = {
    args: {
        isReconnecting: true,
    },
};

export const HealthIssue: StoryObj<CdcSinkProps> = {
    args: {
        hasHealthIssue: true,
    },
};

export const LongValues: StoryObj<CdcSinkProps> = {
    args: {
        hasLongValues: true,
        hasRuntimeError: true,
        hasHealthIssue: true,
    },
    play: expandTaskDetails,
};
