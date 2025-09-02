import { Meta, StoryObj } from "@storybook/react";
import { mockServices } from "test/mocks/services/MockServices";
import { OngoingTasksPage } from "components/pages/database/tasks/ongoingTasks/OngoingTasksPage";
import React from "react";
import { commonInit } from "components/pages/database/tasks/ongoingTasks/stories/common";
import { withBootstrap5, withForceRerender, withStorybookContexts } from "test/storybookTestUtils";
import { userEvent } from "@storybook/test";

export default {
    title: "Pages/Tasks/Ongoing tasks/Internal Replication",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
} satisfies Meta;

interface InternalReplicationProps {
    databaseType: "sharded" | "cluster" | "singleNode";
}

export const Default: StoryObj<InternalReplicationProps> = {
    render: (args: InternalReplicationProps) => {
        commonInit(args.databaseType);

        const { tasksService } = mockServices;

        tasksService.withGetTasks((x) => {
            x.OngoingTasks = [];
            x.PullReplications = [];
            x.SubscriptionsCount = 0;
        });

        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
    args: {
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
    play: async ({ canvas }) => {
        const button = canvas.queryByTitle(/Click for details/);
        if (button) {
            await userEvent.click(button);
        }
    },
};

export const NotSharded: StoryObj<InternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        databaseType: "cluster",
    },
};

export const SingleNode: StoryObj<InternalReplicationProps> = {
    ...Default,
    args: {
        ...Default.args,
        databaseType: "singleNode",
    },
};
