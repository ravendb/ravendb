import { Reducer } from "react";
import {
    OngoingEtlTaskNodeInfo,
    OngoingTaskElasticSearchEtlSharedInfo,
    OngoingTaskExternalReplicationSharedInfo,
    OngoingTaskHubDefinitionInfo,
    OngoingTaskInfo,
    OngoingTaskKafkaEtlSharedInfo,
    OngoingTaskNodeInfo,
    OngoingTaskNodeInfoDetails,
    OngoingTaskNodeProgressDetails,
    OngoingTaskOlapEtlSharedInfo,
    OngoingTaskPeriodicBackupNodeInfoDetails,
    OngoingTaskPeriodicBackupSharedInfo,
    OngoingTaskRabbitMqEtlSharedInfo,
    OngoingTaskRavenEtlSharedInfo,
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskReplicationSinkSharedInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSqlEtlSharedInfo,
    OngoingTaskSubscriptionSharedInfo,
} from "../../../../models/tasks";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import { produce } from "immer";
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import { databaseLocationComparator } from "../../../../utils/common";
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import genUtils from "common/generalUtils";
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtlListView;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtlListView;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtlListView;
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtlListView;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlProcessProgress = Raven.Server.Documents.ETL.Stats.EtlProcessProgress;
import TaskUtils from "../../../../utils/TaskUtils";
import { WritableDraft } from "immer/dist/types/types-external";
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtlListView;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;

interface ActionTasksLoaded {
    location: databaseLocationSpecifier;
    tasks: OngoingTasksResult;
    type: "TasksLoaded";
}

interface ActionProgressLoaded {
    location: databaseLocationSpecifier;
    progress: EtlTaskProgress[];
    type: "ProgressLoaded";
}

interface ActionTasksLoadError {
    type: "TasksLoadError";
    location: databaseLocationSpecifier;
    error: JQueryXHR;
}

interface OngoingTasksState {
    tasks: OngoingTaskInfo[];
    locations: databaseLocationSpecifier[];
    replicationHubs: OngoingTaskHubDefinitionInfo[];
}

type OngoingTaskReducerAction = ActionTasksLoaded | ActionProgressLoaded | ActionTasksLoadError;

const serverWidePrefix = "Server Wide";

function mapProgress(taskProgress: EtlProcessProgress): OngoingTaskNodeProgressDetails {
    const totalItems =
        taskProgress.TotalNumberOfDocuments +
        taskProgress.TotalNumberOfDocumentTombstones +
        taskProgress.TotalNumberOfCounterGroups;

    return {
        documents: {
            processed: taskProgress.TotalNumberOfDocuments - taskProgress.NumberOfDocumentsToProcess,
            total: taskProgress.TotalNumberOfDocuments,
        },
        documentTombstones: {
            processed: taskProgress.TotalNumberOfDocumentTombstones - taskProgress.NumberOfDocumentTombstonesToProcess,
            total: taskProgress.TotalNumberOfDocumentTombstones,
        },
        counterGroups: {
            processed: taskProgress.TotalNumberOfCounterGroups - taskProgress.NumberOfCounterGroupsToProcess,
            total: taskProgress.TotalNumberOfCounterGroups,
        },
        global: {
            processed:
                totalItems -
                taskProgress.NumberOfDocumentsToProcess -
                taskProgress.NumberOfDocumentTombstonesToProcess -
                taskProgress.NumberOfCounterGroupsToProcess,
            total: totalItems,
        },
        transformationName: taskProgress.TransformationName,
        completed: taskProgress.Completed,
        disabled: taskProgress.Disabled,
        processedPerSecond: taskProgress.AverageProcessedPerSecond,
    };
}
function mapSharedInfo(task: OngoingTask): OngoingTaskSharedInfo {
    const taskType = task.TaskType;

    const commonProps: OngoingTaskSharedInfo = {
        taskType: TaskUtils.ongoingTaskToStudioTaskType(task),
        taskName: task.TaskName,
        taskId: task.TaskId,
        mentorNodeTag: task.MentorNode,
        responsibleNodeTag: task.ResponsibleNode?.NodeTag,
        taskState: task.TaskState,
        serverWide: task.TaskName.startsWith(serverWidePrefix),
    };

    switch (taskType) {
        case "Replication": {
            const incoming = task as OngoingTaskReplication;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskExternalReplicationSharedInfo = {
                ...commonProps,
                destinationDatabase: incoming.DestinationDatabase,
                destinationUrl: incoming.DestinationUrl,
                connectionStringName: incoming.ConnectionStringName,
                topologyDiscoveryUrls: incoming.TopologyDiscoveryUrls,
                delayReplicationTime: incoming.DelayReplicationFor
                    ? genUtils.timeSpanToSeconds(incoming.DelayReplicationFor)
                    : null,
            };
            return result;
        }
        case "SqlEtl": {
            const incoming = task as OngoingTaskSqlEtlListView;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskSqlEtlSharedInfo = {
                ...commonProps,
                destinationServer: incoming.DestinationServer,
                destinationDatabase: incoming.DestinationDatabase,
                connectionStringName: incoming.ConnectionStringName,
                connectionStringDefined: incoming.ConnectionStringDefined,
            };
            return result;
        }
        case "RavenEtl": {
            const incoming = task as OngoingTaskRavenEtlListView;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskRavenEtlSharedInfo = {
                ...commonProps,
                destinationDatabase: incoming.DestinationDatabase,
                destinationUrl: incoming.DestinationUrl,
                connectionStringName: incoming.ConnectionStringName,
                topologyDiscoveryUrls: incoming.TopologyDiscoveryUrls,
            };
            return result;
        }
        case "ElasticSearchEtl": {
            const incoming = task as OngoingTaskElasticSearchEtlListView;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskElasticSearchEtlSharedInfo = {
                ...commonProps,
                connectionStringName: incoming.ConnectionStringName,
                nodesUrls: incoming.NodesUrls,
            };
            return result;
        }
        case "QueueEtl": {
            const incoming = task as OngoingTaskQueueEtlListView;
            switch (incoming.BrokerType) {
                case "Kafka": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskKafkaEtlSharedInfo = {
                        ...commonProps,
                        connectionStringName: incoming.ConnectionStringName,
                        url: incoming.Url,
                    };
                    return result;
                }
                case "RabbitMq": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskRabbitMqEtlSharedInfo = {
                        ...commonProps,
                        connectionStringName: incoming.ConnectionStringName,
                        url: incoming.Url,
                    };
                    return result;
                }
                default:
                    throw new Error("Invalid broker type: " + incoming.BrokerType);
            }
        }
        case "Backup": {
            const incoming = task as OngoingTaskBackup;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskPeriodicBackupSharedInfo = {
                ...commonProps,
                backupDestinations: incoming.BackupDestinations,
                lastExecutingNodeTag: incoming.LastExecutingNodeTag,
                lastFullBackup: incoming.LastFullBackup,
                lastIncrementalBackup: incoming.LastIncrementalBackup,
                backupType: incoming.BackupType,
                encrypted: incoming.IsEncrypted,
                nextBackup: incoming.NextBackup,
                retentionPolicy: incoming.RetentionPolicy,
            };
            return result;
        }
        case "OlapEtl": {
            const incoming = task as OngoingTaskOlapEtlListView;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskOlapEtlSharedInfo = {
                ...commonProps,
                connectionStringName: incoming.ConnectionStringName,
                destinationDescription: incoming.Destination,
                destinations: incoming.Destination?.split(",") ?? [],
            };
            return result;
        }
        case "PullReplicationAsSink":
            const incoming = task as OngoingTaskPullReplicationAsSink;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskReplicationSinkSharedInfo = {
                ...commonProps,
                connectionStringName: incoming.ConnectionStringName,
                destinationDatabase: incoming.DestinationDatabase,
                destinationUrl: incoming.DestinationUrl,
                topologyDiscoveryUrls: incoming.TopologyDiscoveryUrls,
                hubName: incoming.HubName,
                mode: incoming.Mode,
            };
            return result;
        case "PullReplicationAsHub": {
            const incoming = task as OngoingTaskPullReplicationAsHub;

            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskReplicationHubSharedInfo = {
                ...commonProps,
                destinationDatabase: incoming.DestinationDatabase,
                destinationUrl: incoming.DestinationUrl,
            };
            return result;
        }
        case "Subscription": {
            const incoming = task as OngoingTaskSubscription;

            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskSubscriptionSharedInfo = {
                ...commonProps,
                lastClientConnectionTime: incoming.LastClientConnectionTime,
                lastBatchAckTime: incoming.LastBatchAckTime,
                changeVectorForNextBatchStartingPointPerShard: incoming.ChangeVectorForNextBatchStartingPointPerShard,
                changeVectorForNextBatchStartingPoint: incoming.ChangeVectorForNextBatchStartingPoint,
            };
            return result;
        }
    }

    return commonProps;
}

function mapNodeInfo(task: OngoingTask): OngoingTaskNodeInfoDetails {
    const commonProps: OngoingTaskNodeInfoDetails = {
        taskConnectionStatus: task.TaskConnectionStatus,
        responsibleNode: task.ResponsibleNode?.NodeTag,
        error: task.Error,
    };
    switch (task.TaskType) {
        case "Backup": {
            const incoming = task as OngoingTaskBackup;
            return {
                ...commonProps,
                onGoingBackup: incoming.OnGoingBackup,
            } as OngoingTaskPeriodicBackupNodeInfoDetails;
        }

        default:
            return commonProps;
    }
}

function initNodesInfo(locations: databaseLocationSpecifier[]): OngoingTaskNodeInfo[] {
    return locations.map((l) => ({
        location: l,
        status: "notLoaded",
        details: null,
    }));
}

export const ongoingTasksReducer: Reducer<OngoingTasksState, OngoingTaskReducerAction> = (
    state: OngoingTasksState,
    action: OngoingTaskReducerAction
): OngoingTasksState => {
    switch (action.type) {
        case "TasksLoaded": {
            const incomingLocation = action.location;
            const incomingTasks = action.tasks;

            return produce(state, (draft) => {
                const newTasks = incomingTasks.OngoingTasksList.map((incomingTask) => {
                    const incomingTaskType = TaskUtils.ongoingTaskToStudioTaskType(incomingTask);
                    const existingTask = state.tasks.find(
                        (x) => x.shared.taskType === incomingTaskType && x.shared.taskId === incomingTask.TaskId
                    );

                    const nodesInfo = existingTask ? existingTask.nodesInfo : initNodesInfo(state.locations);
                    const existingNodeInfo = existingTask
                        ? existingTask.nodesInfo.find((x) => databaseLocationComparator(x.location, incomingLocation))
                        : null;

                    const newNodeInfo: OngoingTaskNodeInfo = {
                        location: incomingLocation,
                        status: "loaded",
                        details: mapNodeInfo(incomingTask),
                    };

                    if (existingNodeInfo) {
                        const { location, status, details, ...restProps } = existingNodeInfo;
                        // retain other props - like etlProgress
                        Object.assign(newNodeInfo, restProps);
                    }

                    return {
                        shared: mapSharedInfo(incomingTask),
                        nodesInfo: [
                            ...nodesInfo.map((x) =>
                                databaseLocationComparator(x.location, newNodeInfo.location) ? newNodeInfo : x
                            ),
                        ],
                    };
                });

                newTasks.sort((a: OngoingTaskInfo, b: OngoingTaskInfo) =>
                    genUtils.sortAlphaNumeric(a.shared.taskName, b.shared.taskName)
                );

                draft.tasks = newTasks;

                draft.replicationHubs = incomingTasks.PullReplications.map((incomingTask) => {
                    return {
                        shared: {
                            taskId: incomingTask.TaskId,
                            taskName: incomingTask.Name,
                            taskState: incomingTask.Disabled ? "Disabled" : "Enabled",
                            delayReplicationTime: incomingTask.DelayReplicationFor
                                ? genUtils.timeSpanToSeconds(incomingTask.DelayReplicationFor)
                                : null,
                            taskMode: incomingTask.Mode,
                            hasFiltering: incomingTask.WithFiltering,
                            serverWide: incomingTask.Name.startsWith(serverWidePrefix),
                            taskType: "PullReplicationAsHub",
                            mentorNodeTag: null,
                            responsibleNodeTag: null,
                        },
                        nodesInfo: undefined,
                    };
                });
            });
        }
        case "TasksLoadError": {
            const incomingLocation = action.location;
            const error = action.error;

            return produce(state, (draft) => {
                draft.tasks.forEach((task) => {
                    const nodeInfo = task.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );
                    nodeInfo.status = "error";
                    nodeInfo.details = {
                        error: error.responseJSON.Message,
                        responsibleNode: null,
                        taskConnectionStatus: null,
                    };
                });
            });
        }
        case "ProgressLoaded": {
            const incomingProgress = action.progress;
            const incomingLocation = action.location;

            return produce(state, (draft) => {
                draft.tasks.forEach((task) => {
                    const perLocationDraft = task.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );
                    const progressToApply = incomingProgress.find(
                        (x) =>
                            TaskUtils.etlTypeToTaskType(x.EtlType) ===
                                TaskUtils.studioTaskTypeToTaskType(task.shared.taskType) &&
                            x.TaskName === task.shared.taskName
                    );
                    (perLocationDraft as WritableDraft<OngoingEtlTaskNodeInfo>).etlProgress = progressToApply
                        ? progressToApply.ProcessesProgress.map(mapProgress)
                        : null;
                });
            });
        }
    }

    return state;
};

export const ongoingTasksReducerInitializer = (locations: databaseLocationSpecifier[]): OngoingTasksState => {
    return {
        tasks: [],
        replicationHubs: [],
        locations,
    };
};
