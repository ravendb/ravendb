import { mockServices } from "test/mocks/services/MockServices";
import { TasksStubs } from "test/stubs/TasksStubs";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import { userEvent, within } from "storybook/test";
import React from "react";
import { commonInit } from "components/pages/database/tasks/ongoingTasks/stories/common";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
import { MockedValue } from "test/mocks/services/AutoMockService";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { mockStore } from "test/mocks/store/MockStore";

export default {
    title: "Pages/Tasks/Ongoing tasks/Subscription",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface SubscriptionProps {
    disabled: boolean;
    completed: boolean;
    customizeTask: (x: OngoingTaskSubscription) => void;
    runtimeError: boolean;
    loadError: boolean;
    licenseLimit: boolean;
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<SubscriptionProps> = {
    render: (args: SubscriptionProps) => {
        commonInit(args.databaseType);

        const { license } = mockStore;
        if (args.licenseLimit) {
            license.with_LicenseLimited();
        }

        const { tasksService } = mockServices;

        const mockedValue: MockedValue<OngoingTasksResult> = (x) => {
            const ongoingTask = TasksStubs.getSubscription();
            if (args.disabled) {
                ongoingTask.TaskState = "Disabled";
                ongoingTask.TaskConnectionStatus = "NotActive";
            }
            if (args.runtimeError) {
                ongoingTask.Error = "This is some error";
            }
            if (args.databaseType === "sharded") {
                ongoingTask.ChangeVectorForNextBatchStartingPoint = null;
                ongoingTask.ChangeVectorForNextBatchStartingPointPerShard = {
                    "1": "B:884-7YtyJhmi/k+as1eW7RRJWQ, A:856-TtyicrkQAUKtvYiwGx0yoA",
                    "0": "B:884-7YtyJhmi/k+as1eW7RRJWQ, A:856-TtyicrkQAUKtvYiwGx0yoA",
                    "2": "B:884-7YtyJhmi/k+as1eW7RRJWQ, A:856-TtyicrkQAUKtvYiwGx0yoA",
                };
            }
            args.customizeTask?.(ongoingTask);
            if (args.licenseLimit) {
                x.OngoingTasks = [
                    ongoingTask,
                    {
                        ...TasksStubs.getSubscription(),
                        TaskName: "SomeSecondSub",
                    },
                ];
            } else {
                x.OngoingTasks = [ongoingTask];
            }

            x.PullReplications = [];
            x.SubscriptionsCount = x.OngoingTasks.length;
        };

        if (args.loadError) {
            tasksService.withThrowingGetTasks((db, location) => location.nodeTag === "C", mockedValue);
        } else {
            tasksService.withGetTasks(mockedValue);
        }

        return <OngoingTasksPage />;
    },
    args: {
        completed: true,
        disabled: false,
        runtimeError: false,
        loadError: false,
        licenseLimit: false,
        customizeTask: undefined,
        databaseType: "cluster",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
    play: async ({ canvas }) => {
        const container = within(await canvas.findByTestId("subscriptions"));
        await userEvent.click((await container.findAllByTitle(/Click for details/))[0]);
    },
};

export const WithLicenseLimits: StoryObj<SubscriptionProps> = {
    ...Default,
    args: {
        ...Default.args,
        licenseLimit: true,
    },
};

export const Disabled: StoryObj<SubscriptionProps> = {
    ...Default,
    args: {
        ...Default.args,
        disabled: true,
    },
};

export const Sharded: StoryObj<SubscriptionProps> = {
    ...Default,
    args: {
        ...Default.args,
        databaseType: "sharded",
    },
};
