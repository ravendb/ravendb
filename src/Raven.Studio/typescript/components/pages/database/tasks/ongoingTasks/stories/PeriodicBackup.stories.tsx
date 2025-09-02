import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "../OngoingTasksPage";
import React from "react";
import { commonInit } from "./common";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import { Meta, StoryObj } from "@storybook/react";
import { userEvent, within } from "@storybook/test";
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

export default {
    title: "Pages/Tasks/Ongoing tasks/Period Backup",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface PeriodBackupProps {
    disabled: boolean;
    completed: boolean;
    customizeTask: (x: OngoingTaskBackup) => void;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<PeriodBackupProps> = {
    render: (args: PeriodBackupProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getPeriodicBackupListItem();
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
        const container = within(await canvas.findByTestId("backups"));
        await userEvent.click(await container.findByTitle(/Click for details/));
    },
};

export const Disabled: StoryObj<PeriodBackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const Encrypted: StoryObj<PeriodBackupProps> = {
    ...Default,
    args: {
        ...Default.args,
        customizeTask: (x) => (x.IsEncrypted = true),
    },
};
