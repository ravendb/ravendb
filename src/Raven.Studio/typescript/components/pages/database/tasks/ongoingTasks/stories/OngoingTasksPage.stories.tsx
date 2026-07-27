import React from "react";
import { OngoingTasksPage } from "../OngoingTasksPage";
import { Meta, StoryObj } from "@storybook/react-webpack5";
import { withStorybookContexts, withBootstrap5, withForceRerender } from "test/storybookTestUtils";
import { mockServices } from "test/mocks/services/MockServices";
import { mockStore } from "test/mocks/store/MockStore";
import {
    commonInit,
    mockEtlProgress,
    mockExternalReplicationProgress,
} from "components/pages/database/tasks/ongoingTasks/stories/common";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;

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

const shardResponsibleNodes: Record<number, string> = { 0: "A", 1: "B", 2: "C" };

function applyShardResponsibleNodes(dto: OngoingTasksResult, location: databaseLocationSpecifier) {
    const nodeTag =
        location.shardNumber != null
            ? (shardResponsibleNodes[location.shardNumber] ?? location.nodeTag)
            : location.nodeTag;
    dto.OngoingTasks.forEach((task) => {
        if (task.ResponsibleNode) {
            task.ResponsibleNode = {
                NodeTag: nodeTag,
                NodeUrl: `http://${nodeTag.toLowerCase()}.ravendb`,
                ResponsibleNode: nodeTag,
            };
        }
    });
}

export const FullView: StoryObj<{ isAiOnly: boolean; databaseType: "sharded" | "cluster" | "singleNode" }> = {
    render: (props) => {
        commonInit(props.databaseType);

        const { tasksService } = mockServices;

        tasksService.withGetTasksPerLocation(applyShardResponsibleNodes);
        tasksService.withGetEtlProgress();
        tasksService.withTaskErrors();
        tasksService.withEtlStats();
        tasksService.withGetExternalReplicationProgress();
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage isAiOnly={props.isAiOnly} />;
    },
    args: {
        isAiOnly: false,
        databaseType: "sharded",
    },
    argTypes: {
        databaseType: { control: "radio", options: ["sharded", "cluster", "singleNode"] },
    },
};

export const Completed: StoryObj = {
    render: () => {
        commonInit("sharded");

        const { tasksService } = mockServices;

        tasksService.withGetTasksPerLocation(applyShardResponsibleNodes);
        mockEtlProgress(tasksService, true, false, false);
        tasksService.withTaskErrors();
        tasksService.withEtlStats();
        mockExternalReplicationProgress(tasksService, true);
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
};

export const Disabled: StoryObj = {
    render: () => {
        commonInit("sharded");

        const { tasksService } = mockServices;

        tasksService.withGetTasksPerLocation((dto, location) => {
            applyShardResponsibleNodes(dto, location);
            dto.OngoingTasks.forEach((task) => {
                task.TaskState = "Disabled";
                task.TaskConnectionStatus = "NotActive";
            });
        });
        mockEtlProgress(tasksService, false, true, false);
        tasksService.withTaskErrors([]);
        tasksService.withEtlStats();
        tasksService.withGetExternalReplicationProgress();
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
};

export const WithRuntimeError: StoryObj = {
    render: () => {
        commonInit("sharded");

        const { tasksService } = mockServices;

        tasksService.withGetTasksPerLocation((dto, location) => {
            applyShardResponsibleNodes(dto, location);
            dto.OngoingTasks.forEach((task) => {
                task.Error = "Connection refused: error connecting to remote server at http://target:8080";
            });
        });
        tasksService.withGetEtlProgress();
        tasksService.withTaskErrors();
        tasksService.withEtlStats();
        tasksService.withGetExternalReplicationProgress();
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
};

// Shard 1 (responsible node B) fails to load — shard 0 (A) and shard 2 (C) show normally
export const WithLoadError: StoryObj = {
    render: () => {
        commonInit("sharded");

        const { tasksService } = mockServices;

        tasksService.withGetTasksPerLocation(
            applyShardResponsibleNodes,
            (location) => location.shardNumber === 1 && location.nodeTag === "B"
        );
        tasksService.withGetEtlProgress();
        tasksService.withTaskErrors();
        tasksService.withEtlStats();
        tasksService.withGetExternalReplicationProgress();
        tasksService.withGetInternalReplicationProgress();

        return <OngoingTasksPage />;
    },
};

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
