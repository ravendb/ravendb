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
import MockTasksService from "../../../../../test/mocks/MockTasksService";
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;

function tasksHolder(storyFn: any) {
    return (
        <div
            className="destinations flex-vertical absolute-fill content-margin manage-ongoing-tasks"
            style={{ height: "100vh", overflow: "auto" }}
        >
            {storyFn()}
        </div>
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

export const ExternalReplicationTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskReplication) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ongoingTask = TasksStubs.getExternalReplicationListItem();
        if (args.disabled) {
            ongoingTask.TaskState = "Disabled";
        }
        args.customizeTask?.(ongoingTask);
        x.OngoingTasksList = [ongoingTask];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const ExternalReplicationDisabled = boundCopy(ExternalReplicationTemplate, {
    disabled: true,
});

export const ExternalReplicationEnabled = boundCopy(ExternalReplicationTemplate, {
    disabled: false,
});

export const ExternalReplicationServerWide = boundCopy(ExternalReplicationTemplate, {
    disabled: false,
    customizeTask: (task) => {
        task.TaskName = "Server Wide External Replication, ext1";
    },
});

export const SubscriptionTemplate = (args: {
    disabled?: boolean;
    customizeTask?: (x: OngoingTaskSubscription) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ongoingTask = TasksStubs.getSubscriptionListItem();
        if (args.disabled) {
            ongoingTask.TaskState = "Disabled";
        }
        args.customizeTask?.(ongoingTask);
        x.OngoingTasksList = [ongoingTask];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const SubscriptionDisabled = boundCopy(SubscriptionTemplate, {
    disabled: true,
});

export const SubscriptionEnabled = boundCopy(SubscriptionTemplate, {
    disabled: false,
});

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
        x.PullReplications = [];
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
        x.PullReplications = [];
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
        x.PullReplications = [];
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
        x.PullReplications = [];
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

export const KafkaTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskQueueEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const etl = TasksStubs.getKafkaListItem();
        if (args.disabled) {
            etl.TaskState = "Disabled";
        }
        args.customizeTask?.(etl);
        x.OngoingTasksList = [etl];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const KafkaDisabled = boundCopy(KafkaTemplate, {
    disabled: true,
});

export const KafkaCompleted = boundCopy(KafkaTemplate, {
    completed: true,
});

export const KafkaEmptyScript = boundCopy(KafkaTemplate, {
    completed: true,
    emptyScript: true,
});

export const RabbitTemplate = (args: {
    disabled?: boolean;
    completed?: boolean;
    emptyScript?: boolean;
    customizeTask?: (x: OngoingTaskQueueEtlListView) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const etl = TasksStubs.getRabbitListItem();
        if (args.disabled) {
            etl.TaskState = "Disabled";
        }
        args.customizeTask?.(etl);
        x.OngoingTasksList = [etl];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    mockEtlProgress(tasksService, args.completed, args.disabled, args.emptyScript);

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const RabbitDisabled = boundCopy(RabbitTemplate, {
    disabled: true,
});

export const RabbitCompleted = boundCopy(RabbitTemplate, {
    completed: true,
});

export const RabbitEmptyScript = boundCopy(RabbitTemplate, {
    completed: true,
    emptyScript: true,
});

export const ReplicationSinkTemplate = (args: {
    disabled?: boolean;
    customizeTask?: (x: OngoingTaskPullReplicationAsSink) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const sinkListItem = TasksStubs.getReplicationSinkListItem();
        if (args.disabled) {
            sinkListItem.TaskState = "Disabled";
        }
        args.customizeTask?.(sinkListItem);
        x.OngoingTasksList = [sinkListItem];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const ReplicationSinkDisabled = boundCopy(ReplicationSinkTemplate, {
    disabled: true,
});

export const ReplicationSinkEnabled = boundCopy(ReplicationSinkTemplate, {
    disabled: false,
});

export const ReplicationHubTemplate = (args: {
    disabled?: boolean;
    withOutConnections?: boolean;
    customizeTask?: (x: OngoingTaskPullReplicationAsHub) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const listItem = TasksStubs.getReplicationHubListItem();
        if (args.disabled) {
            listItem.TaskState = "Disabled";
        }

        x.PullReplications.forEach((definition) => {
            definition.Disabled = args.disabled;
        });

        args.customizeTask?.(listItem);
        x.OngoingTasksList = args.withOutConnections ? [] : [listItem];
        x.PullReplications = x.PullReplications.filter((x) =>
            args.withOutConnections ? x.Name === "EmptyHub" : x.Name !== "EmptyHub"
        );
        x.SubscriptionsCount = 0;
    });

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const ReplicationHubDisabled = boundCopy(ReplicationHubTemplate, {
    disabled: true,
});

export const ReplicationHubEnabled = boundCopy(ReplicationHubTemplate, {
    disabled: false,
});

export const ReplicationHubNoConnections = boundCopy(ReplicationHubTemplate, {
    disabled: false,
    withOutConnections: true,
});

export const PeriodicBackupTemplate = (args: {
    disabled?: boolean;
    customizeTask?: (x: OngoingTaskBackup) => void;
}) => {
    const db = DatabasesStubs.shardedDatabase();

    commonInit();

    const { tasksService } = mockServices;

    tasksService.withGetTasks((x) => {
        const ongoingTask = TasksStubs.getPeriodicBackupListItem();
        if (args.disabled) {
            ongoingTask.TaskState = "Disabled";
        }
        args.customizeTask?.(ongoingTask);
        x.OngoingTasksList = [ongoingTask];
        x.PullReplications = [];
        x.SubscriptionsCount = 0;
    });

    return <OngoingTasksPage {...forceStoryRerender()} database={db} />;
};

export const PeriodicBackupDisabled = boundCopy(PeriodicBackupTemplate, {
    disabled: true,
});

export const PeriodicBackupEnabledEncrypted = boundCopy(PeriodicBackupTemplate, {
    disabled: false,
    customizeTask: (x) => (x.IsEncrypted = true),
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
