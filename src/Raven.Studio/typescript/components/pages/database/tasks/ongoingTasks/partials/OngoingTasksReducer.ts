import { Reducer } from "react";
import {
    OngoingEtlTaskNodeInfo,
    OngoingTaskAzureQueueStorageEtlSharedInfo,
    OngoingTaskElasticSearchEtlSharedInfo,
    OngoingTaskExternalReplicationSharedInfo,
    OngoingTaskHubDefinitionInfo,
    OngoingTaskInfo,
    OngoingTaskKafkaEtlSharedInfo,
    OngoingTaskKafkaSinkSharedInfo,
    OngoingTaskNodeInfo,
    OngoingTaskNodeInfoDetails,
    OngoingTaskNodeEtlProgressDetails,
    OngoingTaskOlapEtlSharedInfo,
    OngoingTaskPeriodicBackupNodeInfoDetails,
    OngoingTaskPeriodicBackupSharedInfo,
    OngoingTaskRabbitMqEtlSharedInfo,
    OngoingTaskRabbitMqSinkSharedInfo,
    OngoingTaskRavenEtlSharedInfo,
    OngoingTaskReplicationHubInfo,
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskReplicationSinkSharedInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSqlEtlSharedInfo,
    OngoingTaskSnowflakeEtlSharedInfo,
    OngoingTaskSubscriptionInfo,
    OngoingTaskSubscriptionSharedInfo,
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskNodeReplicationProgressDetails,
    OngoingTaskExternalReplicationNodeInfoDetails,
    OngoingTaskReplicationHubNodeInfoDetails,
    OngoingInternalReplicationNodeInfo,
    OngoingTaskAmazonSqsEtlSharedInfo,
    OngoingTaskCdcSinkSharedInfo,
    OngoingTaskEmbeddingsGenerationSharedInfo,
    OngoingTaskGenAiSharedInfo,
} from "components/models/tasks";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import { databaseLocationComparator } from "components/utils/common";
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import genUtils from "common/generalUtils";
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl;
import OngoingTaskSnowflakeEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSnowflakeEtl;
import EmbeddingsGeneration = Raven.Client.Documents.Operations.OngoingTasks.EmbeddingsGeneration;
import GenAi = Raven.Client.Documents.Operations.OngoingTasks.GenAi;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl;
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlProcessProgress = Raven.Server.Documents.ETL.Stats.EtlProcessProgress;
import { produce, Draft } from "immer";
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl;
import OngoingTaskQueueSinkListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink;
import OngoingTaskCdcSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskCdcSink;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import SubscriptionConnectionsDetails = Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails;
import DatabaseUtils from "components/utils/DatabaseUtils";
import { DatabaseSharedInfo } from "components/models/databases";
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import ReplicationProcessProgress = Raven.Server.Documents.Replication.Stats.ReplicationProcessProgress;
import InternalReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.InternalReplicationTaskProgress;
import TaskUtils from "components/utils/TaskUtils";
import { sortBy } from "common/typeUtils";

interface ActionTasksLoaded {
    location: databaseLocationSpecifier;
    tasks: OngoingTasksResult;
    type: "TasksLoaded";
}

interface ActionTaskLoaded {
    nodeTag: string;
    task: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
    type: "SubscriptionInfoLoaded";
}

interface ActionSubscriptionConnectionDetailsLoaded {
    type: "SubscriptionConnectionDetailsLoaded";
    subscriptionId: number;
    loadError?: string;
    details?: SubscriptionConnectionsDetails;
}

interface ActionEtlProgressLoaded {
    location: databaseLocationSpecifier;
    progress: EtlTaskProgress[];
    type: "EtlProgressLoaded";
}

interface ActionReplicationProgressLoaded {
    location: databaseLocationSpecifier;
    progress: ReplicationTaskProgress[];
    type: "ReplicationProgressLoaded";
}

interface ActionInternalReplicationProgressLoaded {
    location: databaseLocationSpecifier;
    progress: InternalReplicationTaskProgress[];
    type: "InternalReplicationProgressLoaded";
}

interface ActionInternalReplicationProgressError {
    location: databaseLocationSpecifier;
    error: string;
    type: "InternalReplicationProgressError";
}

interface ActionTasksLoadError {
    type: "TasksLoadError";
    location: databaseLocationSpecifier;
    error: string;
}

export interface OngoingTasksState {
    tasks: OngoingTaskInfo[];
    subscriptions: OngoingTaskSubscriptionInfo[];
    locations: databaseLocationSpecifier[];
    orchestrators: string[];
    replicationHubs: OngoingTaskHubDefinitionInfo[];
    subscriptionConnectionDetails: SubscriptionConnectionsDetailsWithId[];
    internalReplication: OngoingInternalReplicationNodeInfo[];
}

export type SubscriptionConnectionsDetailsWithId = SubscriptionConnectionsDetails & {
    SubscriptionId: number;
    LoadError?: string;
};

type OngoingTaskReducerAction =
    | ActionTasksLoaded
    | ActionEtlProgressLoaded
    | ActionReplicationProgressLoaded
    | ActionInternalReplicationProgressLoaded
    | ActionInternalReplicationProgressError
    | ActionTasksLoadError
    | ActionTaskLoaded
    | ActionSubscriptionConnectionDetailsLoaded;

const serverWidePrefix = "Server Wide";

// for external replication hubs we might have multiple tasks with same taskId
// thus we need better comparator for those items
const uniqueIdExtractor = (task: OngoingTaskInfo) => {
    if (task.shared.taskType !== "PullReplicationAsHub") {
        return task.shared.taskId;
    }
    const hubTask = task as OngoingTaskReplicationHubInfo;
    return (
        hubTask.shared.taskId +
        "_" +
        (hubTask.shared.destinationDatabase ?? "??") +
        "_" +
        (hubTask.shared.destinationUrl ?? "??")
    );
};

const uniqueIdExtractorRaw = (task: OngoingTask) => {
    if (task.TaskType !== "PullReplicationAsHub") {
        return task.TaskId;
    }

    const hubTask = task as OngoingTaskPullReplicationAsHub;

    return hubTask.TaskId + "_" + (hubTask.DestinationDatabase ?? "??") + "_" + (hubTask.DestinationUrl ?? "??");
};

function mapEtlProgress(taskProgress: EtlProcessProgress): OngoingTaskNodeEtlProgressDetails {
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
        transactionalId: taskProgress.TransactionalId,
    };
}

function mapReplicationProgress(taskProgress: ReplicationProcessProgress): OngoingTaskNodeReplicationProgressDetails {
    const totalItems =
        taskProgress.TotalNumberOfAttachments +
        taskProgress.TotalNumberOfCounterGroups +
        taskProgress.TotalNumberOfDocuments +
        taskProgress.TotalNumberOfDocumentTombstones +
        taskProgress.TotalNumberOfRevisions +
        taskProgress.TotalNumberOfTimeSeriesDeletedRanges +
        taskProgress.TotalNumberOfTimeSeriesSegments;

    return {
        completed: taskProgress.Completed,
        global: {
            processed:
                totalItems -
                taskProgress.NumberOfAttachmentsToProcess -
                taskProgress.NumberOfCounterGroupsToProcess -
                taskProgress.NumberOfDocumentsToProcess -
                taskProgress.NumberOfDocumentTombstonesToProcess -
                taskProgress.NumberOfRevisionsToProcess -
                taskProgress.NumberOfTimeSeriesDeletedRangesToProcess -
                taskProgress.NumberOfTimeSeriesSegmentsToProcess,
            total: totalItems,
        },
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
        attachments: {
            processed: taskProgress.TotalNumberOfAttachments - taskProgress.NumberOfAttachmentsToProcess,
            total: taskProgress.TotalNumberOfAttachments,
        },
        revisions: {
            processed: taskProgress.TotalNumberOfRevisions - taskProgress.NumberOfRevisionsToProcess,
            total: taskProgress.TotalNumberOfRevisions,
        },
        timeSeriesDeletedRanges: {
            processed:
                taskProgress.TotalNumberOfTimeSeriesDeletedRanges -
                taskProgress.NumberOfTimeSeriesDeletedRangesToProcess,
            total: taskProgress.TotalNumberOfTimeSeriesDeletedRanges,
        },
        timeSeries: {
            processed: taskProgress.TotalNumberOfTimeSeriesSegments - taskProgress.NumberOfTimeSeriesSegmentsToProcess,
            total: taskProgress.TotalNumberOfTimeSeriesSegments,
        },
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
        case "SnowflakeEtl": {
            const incoming = task as OngoingTaskSnowflakeEtlListView;
            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskSnowflakeEtlSharedInfo = {
                ...commonProps,
                connectionStringName: incoming.ConnectionStringName,
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
                case "AzureQueueStorage": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskAzureQueueStorageEtlSharedInfo = {
                        ...commonProps,
                        connectionStringName: incoming.ConnectionStringName,
                        url: incoming.Url,
                    };
                    return result;
                }
                case "AmazonSqs": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskAmazonSqsEtlSharedInfo = {
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
        case "QueueSink": {
            const incoming = task as OngoingTaskQueueSinkListView;
            switch (incoming.BrokerType) {
                case "Kafka": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskKafkaSinkSharedInfo = {
                        ...commonProps,
                        connectionStringName: incoming.ConnectionStringName,
                        url: incoming.Url,
                    };
                    return result;
                }
                case "RabbitMq": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskRabbitMqSinkSharedInfo = {
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
        case "CdcSink": {
            const incoming = task as OngoingTaskCdcSink;
            const result: OngoingTaskCdcSinkSharedInfo = {
                ...commonProps,
                connectionStringName: incoming.ConnectionStringName,
                factoryName: incoming.FactoryName,
            };
            return result;
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
        case "PullReplicationAsSink": {
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
        }
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
        case "EmbeddingsGeneration": {
            const incoming = task as EmbeddingsGeneration;

            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskEmbeddingsGenerationSharedInfo = {
                ...commonProps,
                identifier: incoming.Configuration.Identifier,
                connectionStringName: incoming.ConnectionStringName,
            };

            return result;
        }
        case "GenAi": {
            const incoming = task as GenAi;

            // noinspection UnnecessaryLocalVariableJS
            const result: OngoingTaskGenAiSharedInfo = {
                ...commonProps,
                identifier: incoming.Configuration.Identifier,
                connectionStringName: incoming.Configuration.ConnectionStringName,
                nextBatchStartingPoint: incoming.ChangeVector,
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
        //TODO: sink?
        case "Replication": {
            const incoming = task as OngoingTaskReplication;
            return {
                ...commonProps,
                fromToString: incoming.FromToString,
                lastAcceptedChangeVectorFromDestination: incoming.LastAcceptedChangeVectorFromDestination,
                lastSentEtag: incoming.LastSentEtag,
                lastDatabaseEtag: incoming.LastDatabaseEtag,
                sourceDatabaseChangeVector: incoming.SourceDatabaseChangeVector,
            } as OngoingTaskExternalReplicationNodeInfoDetails;
        }
        case "PullReplicationAsHub": {
            const incoming = task as OngoingTaskPullReplicationAsHub;
            return {
                ...commonProps,
                handlerId: incoming.HandlerId,
                fromToString: incoming.FromToString,
                lastAcceptedChangeVectorFromDestination: incoming.LastAcceptedChangeVectorFromDestination,
                lastSentEtag: incoming.LastSentEtag,
                lastDatabaseEtag: incoming.LastDatabaseEtag,
                sourceDatabaseChangeVector: incoming.SourceDatabaseChangeVector,
            } as OngoingTaskReplicationHubNodeInfoDetails;
        }

        default:
            return commonProps;
    }
}

function initNodesInfo(
    taskType: StudioTaskType,
    locations: databaseLocationSpecifier[],
    orchestrators: string[]
): OngoingTaskNodeInfo[] {
    const sharded = orchestrators.length > 0;
    if (sharded && taskType === "Subscription") {
        return orchestrators.map(
            (nodeTag): OngoingTaskNodeInfo => ({
                location: {
                    nodeTag,
                },
                status: "idle",
                details: null,
            })
        );
    }

    return locations.map(
        (l): OngoingTaskNodeInfo => ({
            location: l,
            status: "idle",
            details: null,
        })
    );
}

const mapTask = (
    incomingTask: OngoingTask,
    incomingLocation: databaseLocationSpecifier,
    state: OngoingTasksState
): OngoingTaskInfo => {
    const incomingTaskType = TaskUtils.ongoingTaskToStudioTaskType(incomingTask);

    const existingTasksSource = incomingTask.TaskType === "Subscription" ? state.subscriptions : state.tasks;
    const existingTask = existingTasksSource.find(
        (x) => x.shared.taskType === incomingTaskType && uniqueIdExtractor(x) === uniqueIdExtractorRaw(incomingTask)
    );

    const nodesInfo = existingTask
        ? existingTask.nodesInfo
        : initNodesInfo(incomingTaskType, state.locations, state.orchestrators);
    const existingNodeInfo = existingTask
        ? existingTask.nodesInfo.find((x) => databaseLocationComparator(x.location, incomingLocation))
        : null;

    const newNodeInfo: OngoingTaskNodeInfo = {
        location: incomingLocation,
        status: "success",
        details: mapNodeInfo(incomingTask),
    };

    if (existingNodeInfo) {
        // eslint-disable-next-line @typescript-eslint/no-unused-vars
        const { location, status, details, ...restProps } = existingNodeInfo;
        // retain other props - like etlProgress
        Object.assign(newNodeInfo, restProps);
    }

    const responsibleNode = incomingTask.ResponsibleNode?.NodeTag;
    const responsibleLocation: databaseLocationSpecifier = responsibleNode
        ? { shardNumber: incomingLocation.shardNumber, nodeTag: responsibleNode }
        : null;

    return {
        shared: mapSharedInfo(incomingTask),
        nodesInfo: [
            ...nodesInfo.map((x) => (databaseLocationComparator(x.location, newNodeInfo.location) ? newNodeInfo : x)),
        ],
        responsibleLocations: responsibleLocation ? [responsibleLocation] : [],
    };
};

export const ongoingTasksReducer: Reducer<OngoingTasksState, OngoingTaskReducerAction> = (
    state: OngoingTasksState,
    action: OngoingTaskReducerAction
): OngoingTasksState => {
    switch (action.type) {
        case "SubscriptionInfoLoaded": {
            const incomingTask = action.task;
            const incomingLocation = action.nodeTag;

            return produce(state, (draft) => {
                const existingTask = draft.subscriptions.find((x) => x.shared.taskId === incomingTask.SubscriptionId);

                if (existingTask) {
                    existingTask.shared.taskState = incomingTask.Disabled ? "Disabled" : "Enabled";
                    existingTask.shared.responsibleNodeTag = incomingTask.ResponsibleNode?.NodeTag;

                    const existingNodeInfo = existingTask.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, { nodeTag: incomingLocation })
                    );

                    if (existingNodeInfo?.details) {
                        existingNodeInfo.details.responsibleNode = incomingTask.ResponsibleNode?.NodeTag;
                        existingNodeInfo.details.taskConnectionStatus = incomingTask.TaskConnectionStatus;
                    }
                }
            });
        }

        case "TasksLoaded": {
            const incomingLocation = action.location;
            const incomingTasks = action.tasks;
            const sharded = state.orchestrators.length > 0;

            return produce(state, (draft) => {
                const newTasks = incomingTasks.OngoingTasks.map((incomingTask) =>
                    mapTask(incomingTask, incomingLocation, state)
                );

                newTasks.sort((a: OngoingTaskInfo, b: OngoingTaskInfo) =>
                    genUtils.sortAlphaNumeric(a.shared.taskName, b.shared.taskName)
                );

                // since endpoint returns information about subscription from (orchestrator <-> target node) point of view
                // we only update subscriptions info when data comes from non-sharded db or sharded db but at node level
                if (!sharded || (sharded && incomingLocation.shardNumber != null)) {
                    const tasksWithoutSubscriptions = newTasks.filter((x) => x.shared.taskType !== "Subscription");

                    tasksWithoutSubscriptions.forEach((task) => {
                        const draftTaskIdx = draft.tasks.findIndex(
                            (x) => uniqueIdExtractor(x) === uniqueIdExtractor(task)
                        );

                        if (draftTaskIdx !== -1) {
                            const draftNodeInfoIdx = draft.tasks[draftTaskIdx].nodesInfo.findIndex((x) =>
                                databaseLocationComparator(x.location, incomingLocation)
                            );
                            const nodeInfoIdx = task.nodesInfo.findIndex((x) =>
                                databaseLocationComparator(x.location, incomingLocation)
                            );

                            if (draftNodeInfoIdx !== -1 && nodeInfoIdx !== -1) {
                                draft.tasks[draftTaskIdx].nodesInfo[draftNodeInfoIdx] = task.nodesInfo[nodeInfoIdx];
                                draft.tasks[draftTaskIdx].shared = task.shared;
                            }

                            let effectiveLocations = draft.tasks[draftTaskIdx].responsibleLocations;

                            task.responsibleLocations.forEach((responsibleLocation) => {
                                effectiveLocations = mergeResponsibleNodes(effectiveLocations, responsibleLocation);
                            });

                            draft.tasks[draftTaskIdx].responsibleLocations = effectiveLocations;
                        } else {
                            // The task is not in the state so we add it
                            draft.tasks.push(task);
                        }
                    });

                    // Clean deleted tasks
                    draft.tasks.forEach((task) => {
                        if (
                            !newTasks.some((x) => x.shared.taskId === task.shared.taskId) &&
                            task.nodesInfo.find((x) => databaseLocationComparator(x.location, incomingLocation))
                                .status !== "idle"
                        ) {
                            const draftTaskIdx = draft.tasks.findIndex(
                                (x) => uniqueIdExtractor(x) === uniqueIdExtractor(task)
                            );
                            draft.tasks.splice(draftTaskIdx, 1);
                        }
                    });
                }

                if (!sharded || (sharded && incomingLocation.shardNumber == null)) {
                    draft.subscriptions = newTasks.filter(
                        (x) => x.shared.taskType === "Subscription"
                    ) as OngoingTaskSubscriptionInfo[];
                }

                draft.replicationHubs = incomingTasks.PullReplications.map(
                    (incomingTask): OngoingTaskHubDefinitionInfo => {
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
                            responsibleLocations: [],
                        };
                    }
                );
            });
        }
        case "TasksLoadError": {
            const incomingLocation = action.location;
            const error = action.error;

            return produce(state, (draft) => {
                draft.tasks.forEach((task) => {
                    const nodeInfo = task.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    ) as Draft<OngoingEtlTaskNodeInfo>;

                    if (!nodeInfo) {
                        console.error("Unable to find nodeInfo for:", incomingLocation);
                        return;
                    }

                    nodeInfo.status = "failure";
                    nodeInfo.details = {
                        error,
                        responsibleNode: null,
                        taskConnectionStatus: null,
                    };
                    nodeInfo.etlProgress = null;
                });
            });
        }

        case "SubscriptionConnectionDetailsLoaded": {
            const incomingDetails = action.details;
            const subscriptionId = action.subscriptionId;
            const loadError = action.loadError;

            return produce(state, (draft) => {
                const existingIdx = draft.subscriptionConnectionDetails.findIndex(
                    (x) => x.SubscriptionId === subscriptionId
                );

                const itemToSet: SubscriptionConnectionsDetailsWithId = {
                    ...incomingDetails,
                    SubscriptionId: subscriptionId,
                    LoadError: loadError,
                };

                if (existingIdx !== -1) {
                    draft.subscriptionConnectionDetails[existingIdx] = itemToSet;
                } else {
                    draft.subscriptionConnectionDetails.push(itemToSet);
                }
            });
        }

        case "EtlProgressLoaded": {
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
                    (perLocationDraft as Draft<OngoingEtlTaskNodeInfo>).etlProgress = progressToApply
                        ? progressToApply.ProcessesProgress.map(mapEtlProgress)
                        : null;
                });
            });
        }

        case "ReplicationProgressLoaded": {
            const incomingProgresses = action.progress;
            const incomingLocation = action.location;

            return produce(state, (draft) => {
                draft.tasks.forEach((task) => {
                    const perLocationDraft = task.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );

                    if (!perLocationDraft) {
                        console.log("Unable to find draft for: ", incomingLocation);
                        return;
                    }

                    const matchedProgress = matchProgresses(task, perLocationDraft, incomingProgresses);

                    (perLocationDraft as Draft<OngoingReplicationProgressAwareTaskNodeInfo>).progress =
                        matchedProgress.map(mapReplicationProgress);
                });
            });
        }

        case "InternalReplicationProgressLoaded": {
            const incomingProgresses = action.progress;
            const incomingLocation = action.location;

            return produce(state, (draft) => {
                const internalReplication = draft.internalReplication;
                const perLocationDraft = internalReplication.find((x) =>
                    databaseLocationComparator(x.location, incomingLocation)
                );

                if (!perLocationDraft) {
                    console.log("Unable to find draft for: ", incomingLocation);
                    return;
                }

                perLocationDraft.status = "success";
                perLocationDraft.error = null;

                (perLocationDraft as Draft<OngoingInternalReplicationNodeInfo>).progress = incomingProgresses.map(
                    (progress) => {
                        const firstProgress = progress.ProcessesProgress[0];
                        return {
                            ...mapReplicationProgress(firstProgress),
                            lastSentEtag: firstProgress.LastSentEtag,
                            lastDatabaseEtag: firstProgress.LastDatabaseEtag,
                            sourceDatabaseChangeVector: firstProgress.SourceChangeVector,
                            lastAcceptedChangeVectorFromDestination: firstProgress.DestinationChangeVector,
                            fromToString: firstProgress.FromToString,
                            destinationNodeTag: progress.DestinationNodeTag,
                        };
                    }
                );
            });
        }

        case "InternalReplicationProgressError": {
            const incomingError = action.error;
            const incomingLocation = action.location;

            return produce(state, (draft) => {
                const internalReplication = draft.internalReplication;
                const perLocationDraft = internalReplication.find((x) =>
                    databaseLocationComparator(x.location, incomingLocation)
                );

                if (!perLocationDraft) {
                    console.log("Unable to find draft for: ", incomingLocation);
                    return;
                }

                perLocationDraft.status = "failure";
                perLocationDraft.progress = [];
                perLocationDraft.error = incomingError;
            });
        }
    }

    return state;
};

/**
 * Since we are getting data from multiple nodes, we want to know actual responsible node
 * The strategy is: last update wins.
 *
 * We keep this information separate for each shard.
 */
function mergeResponsibleNodes(
    existing: databaseLocationSpecifier[],
    incoming: databaseLocationSpecifier
): databaseLocationSpecifier[] {
    if (existing.find((x) => databaseLocationComparator(x, incoming))) {
        return existing;
    }

    const toDelete = existing.find((x) => x.shardNumber === incoming.shardNumber);
    const copy = existing.filter((x) => x !== toDelete);
    copy.push(incoming);

    return sortBy(copy, (x) => x.shardNumber);
}

function matchProgresses(
    task: OngoingTaskInfo,
    nodeInfo: OngoingTaskNodeInfo,
    progresses: ReplicationTaskProgress[]
): ReplicationProcessProgress[] {
    switch (task.shared.taskType) {
        case "PullReplicationAsHub": {
            const hubDraft = nodeInfo as OngoingReplicationProgressAwareTaskNodeInfo;
            const handlerId = hubDraft.details?.handlerId;

            return progresses.flatMap((p) => p.ProcessesProgress.filter((pp) => pp.HandlerId === handlerId));
        }
        case "PullReplicationAsSink": {
            const matchedProgress = progresses.find(
                (x) => x.ReplicationType === "PullAsSink" && x.TaskName === task.shared.taskName
            );
            return matchedProgress?.ProcessesProgress ?? [];
        }
        case "Replication": {
            const matchedProgress = progresses.find(
                (x) => x.ReplicationType === "External" && x.TaskName === task.shared.taskName
            );

            return matchedProgress?.ProcessesProgress ?? [];
        }
        default:
            return [];
    }
}

export const ongoingTasksReducerInitializer = (db: DatabaseSharedInfo): OngoingTasksState => {
    const locations = DatabaseUtils.getLocations(db);
    const orchestrators = db.isSharded ? db.nodes.map((x) => x.tag) : [];

    const internalReplication: OngoingInternalReplicationNodeInfo[] = initNodesInfo(
        "Replication",
        locations,
        orchestrators
    ).map(
        (nodeInfo): OngoingInternalReplicationNodeInfo => ({
            ...nodeInfo,
            progress: [],
            details: null as never,
        })
    );

    return {
        tasks: [],
        subscriptions: [],
        replicationHubs: [],
        locations,
        orchestrators,
        subscriptionConnectionDetails: [],
        internalReplication,
    };
};
