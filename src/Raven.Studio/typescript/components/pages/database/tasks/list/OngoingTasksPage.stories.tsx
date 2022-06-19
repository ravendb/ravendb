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
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView;
import TasksService from "../../../../services/TasksService";
import MockTasksService from "../../../../../test/mocks/MockTasksService";
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView;

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
    emptyScript?: boolean;
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

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const RavenEtlDisabled = boundCopy(RavenEtlTemplate, {
    disabled: true,
});

export const RavenEtlCompleted = boundCopy(RavenEtlTemplate, {
    completed: true,
});

export const RavenEtlEmptyScript = boundCopy(RavenEtlTemplate, {
    completed: true,
    emptyScript: true,
});

export const SqlTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskSqlEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const sqlEtl = TasksStubs.getSqlListItem();
        if (args.disabled) {
            sqlEtl.TaskState = "Disabled";
        }
        args.customizeTask?.(sqlEtl);
        x.OngoingTasksList = [sqlEtl];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const SqlDisabled = boundCopy(SqlTemplate, {
    disabled: true,
});

export const SqlCompleted = boundCopy(SqlTemplate, {
    completed: true,
});

export const SqlEmptyScript = boundCopy(SqlTemplate, {
    completed: true,
    emptyScript: true,
});

export const OlapTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskOlapEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const etl = TasksStubs.getOlapListItem();
        if (args.disabled) {
            etl.TaskState = "Disabled";
        }
        args.customizeTask?.(etl);
        x.OngoingTasksList = [etl];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const OlapDisabled = boundCopy(OlapTemplate, {
    disabled: true,
});

export const OlapCompleted = boundCopy(OlapTemplate, {
    completed: true,
});

export const OlapEmptyScript = boundCopy(OlapTemplate, {
    completed: true,
    emptyScript: true,
});

export const ElasticSearchTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskElasticSearchEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const etl = TasksStubs.getElasticSearchListItem();
        if (args.disabled) {
            etl.TaskState = "Disabled";
        }
        args.customizeTask?.(etl);
        x.OngoingTasksList = [etl];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const ElasticSearchDisabled = boundCopy(ElasticSearchTemplate, {
    disabled: true,
});

export const ElasticSearchCompleted = boundCopy(ElasticSearchTemplate, {
    completed: true,
});

export const ElasticSearchEmptyScript = boundCopy(ElasticSearchTemplate, {
    completed: true,
    emptyScript: true,
});

function mockEtlProgress(tasksService: MockTasksService, completed: boolean, disabled: boolean, emptyScript: boolean) {
    if (completed) {
        tasksService.withGetProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach((progress) => {
                    progress.Completed = true;
                    progress.Disabled = disabled;
                    progress.NumberOfDocumentsToProcess = 0;
                    progress.NumberOfTimeSeriesSegmentsToProcess = 0;
                    progress.NumberOfTimeSeriesDeletedRangesToProcess = 0;
                    progress.NumberOfCounterGroupsToProcess = 0;
                    progress.NumberOfDocumentTombstonesToProcess = 0;
                    if (emptyScript) {
                        progress.TotalNumberOfDocuments = 0;
                        progress.TotalNumberOfTimeSeriesDeletedRanges = 0;
                        progress.TotalNumberOfTimeSeriesSegments = 0;
                        progress.TotalNumberOfDocumentTombstones = 0;
                        progress.TotalNumberOfCounterGroups = 0;
                    }
                });
            });
        });
    } else {
        tasksService.withGetProgress((dto) => {
            dto.Results.forEach((x) => {
                x.ProcessesProgress.forEach((progress) => {
                    progress.Disabled = disabled;
                });
            });
        });
    }
}
