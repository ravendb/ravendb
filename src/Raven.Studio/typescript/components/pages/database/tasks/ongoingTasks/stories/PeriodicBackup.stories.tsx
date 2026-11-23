import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "../OngoingTasksPage";
import React from "react";
import { commonInit } from "./common";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { userEvent, within } from "storybook/test";
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

interface PeriodBackupProps {
    isDisabled: boolean;
    isEncrypted: boolean;
    hasFailedLastBackup: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export default {
    title: "Pages/Tasks/Ongoing tasks/Period Backup",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    render: (args: PeriodBackupProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getPeriodicBackupListItem();
            if (args.isDisabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            ongoingTask.IsEncrypted = args.isEncrypted;
            x.OngoingTasks = [ongoingTask];
            x.PullReplications = [];
            x.SubscriptionsCount = 0;
        };

        tasksService.withGetTasks(mockedValue);
        tasksService.withGetPeriodicBackupStatus(
            args.hasFailedLastBackup ? [TasksStubs.getFailedPeriodicBackupStatus()] : undefined
        );

        return <OngoingTasksPage />;
    },
    args: {
        isDisabled: false,
        isEncrypted: false,
        hasFailedLastBackup: false,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
} satisfies Meta<PeriodBackupProps>;

const expandTaskDetails: StoryObj<PeriodBackupProps>["play"] = async ({ canvas }) => {
    const container = within(await canvas.findByTestId("backups"));
    await userEvent.click(await container.findByTitle(/Click for details/));
};

export const Default: StoryObj<PeriodBackupProps> = {
    play: expandTaskDetails,
};

export const Disabled: StoryObj<PeriodBackupProps> = {
    args: {
        isDisabled: true,
    },
    play: expandTaskDetails,
};

export const LastBackupFailed: StoryObj<PeriodBackupProps> = {
    args: {
        hasFailedLastBackup: true,
    },
};

export const Encrypted: StoryObj<PeriodBackupProps> = {
    args: {
        isEncrypted: true,
    },
    play: expandTaskDetails,
};
