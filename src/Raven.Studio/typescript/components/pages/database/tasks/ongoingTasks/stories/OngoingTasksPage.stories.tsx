import React from "react";
import { OngoingTasksPage } from "../OngoingTasksPage";
import { Meta, StoryObj } from "@storybook/react";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import { commonInit } from "components/pages/database/tasks/ongoingTasks/stories/common";

export default {
    title: "Pages/Tasks/Ongoing Tasks/Ongoing Tasks Page",
    decorators: [withStorybookContexts, withBootstrap5, withForceRerender],
    parameters: {
        design: {
            type: "figma",
            url: "https://www.figma.com/design/Dym4dtdwQ0j4mg9nmC4ZDI/Pages---Ongoing-Tasks?node-id=0-1&t=p61ns0HrN2R1ZUAu-1",
        },
    },
} satisfies Meta;

export const EmptyView: StoryObj = {
    render: () => {
        commonInit();

        const { databases } = mockStore;
        databases.withActiveDatabase_NonSharded_SingleNode();

        const { tasksService } = mockServices;

        tasksService.withGetTasks((dto) => {
            dto.SubscriptionsCount = 0;
            dto.OngoingTasks = [];
            dto.PullReplications = [];
        });
        tasksService.withGetEtlProgress((dto) => {
            dto.Results = [];
        });
        tasksService.withGetInternalReplicationProgress((dto) => {
            dto.Results = [];
        });

        return <OngoingTasksPage />;
    },
};

export const FullView: StoryObj = {
    render: () => {
        commonInit();

        const { tasksService } = mockServices;

        tasksService.withGetTasks();
        tasksService.withGetEtlProgress();
        tasksService.withGetExternalReplicationProgress();
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
};
