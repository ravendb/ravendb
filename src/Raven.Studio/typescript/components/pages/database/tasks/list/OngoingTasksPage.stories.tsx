import React from "react";
import { OngoingTasksPage } from "./OngoingTasksPage";
import { ComponentMeta, ComponentStory } from "@storybook/react";
import { forceStoryRerender, withStorybookContexts } from "../../../../../test/storybookTestUtils";
import { DatabasesStubs } from "../../../../../test/stubs/DatabasesStubs";
import accessManager from "common/shell/accessManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { mockServices } from "../../../../../test/mocks/MockServices";
import { TasksStubs } from "../../../../../test/stubs/TasksStubs";
import { boundCopy } from "../../../../utils/common";
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;

function tasksHolder(storyFn: any) {
    return (
        <div className="destinations flex-vertical absolute-fill content-margin manage-ongoing-tasks">{storyFn()}</div>
    );
}

export default {
    title: "Pages/Ongoing tasks page",
    component: OngoingTasksPage,
    decorators: [withStorybookContexts, tasksHolder],
    excludeStories: /Template$/,
} as ComponentMeta<typeof OngoingTasksPage>;

function commonInit() {
    accessManager.default.securityClearance("ClusterAdmin");
    clusterTopologyManager.default.localNodeTag = ko.pureComputed(() => "A");
}

export const EmptyView: ComponentStory<typeof OngoingTasksPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((dto) => {
        dto.SubscriptionsCount = 0;
        dto.OngoingTasksList = [];
        dto.PullReplications = [];
    });
    tasksService.withGetProgress((dto) => {
        dto.Results = [];
    });

    return <OngoingTasksPage database={db} />;
};

export const FullView: ComponentStory<typeof OngoingTasksPage> = () => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks();
    tasksService.withGetProgress();

    return <OngoingTasksPage database={db} />;
};

export const RavenEtlTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    customizeTask?: (x: OngoingTaskRavenEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ravenEtl = TasksStubs.getRavenEtlListItem();
        if (args.disabled) {
            ravenEtl.TaskState = "Disabled";
        }
        args.customizeTask?.(ravenEtl);
        x.OngoingTasksList = [ravenEtl];
        x.SubscriptionsCount = 0;
    });

    if (args.completed) {
        tasksService.withGetProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach((progress) => {
                    progress.Completed = true;
                    progress.NumberOfDocumentsToProcess = 0;
                    progress.NumberOfTimeSeriesSegmentsToProcess = 0;
                    progress.NumberOfTimeSeriesDeletedRangesToProcess = 0;
                    progress.NumberOfCounterGroupsToProcess = 0;
                    progress.NumberOfDocumentTombstonesToProcess = 0;
                });
            });
        });
    } else {
        tasksService.withGetProgress();
    }

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const RavenEtlDisabled = boundCopy(RavenEtlTemplate, {
    disabled: true,
});

export const RavenEtlCompleted = boundCopy(RavenEtlTemplate, {
    completed: true,
});
