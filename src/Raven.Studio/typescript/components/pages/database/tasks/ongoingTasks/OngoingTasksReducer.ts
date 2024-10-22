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
    OngoingTaskSubscriptionInfo,
    OngoingTaskSubscriptionSharedInfo,
    OngoingReplicationProgressAwareTaskNodeInfo,
    OngoingTaskNodeReplicationProgressDetails,
} from "components/models/tasks";
import OngoingTasksResult = Raven.Server.Web.System.OngoingTasksResult;
import OngoingTask = Raven.Client.Documents.Operations.OngoingTasks.OngoingTask;
import { databaseLocationComparator } from "components/utils/common";
import OngoingTaskReplication = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskReplication;
import genUtils from "common/generalUtils";
import OngoingTaskSqlEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSqlEtl;
import OngoingTaskRavenEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskRavenEtl;
import OngoingTaskElasticSearchEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskElasticSearchEtl;
import OngoingTaskOlapEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskOlapEtl;
import OngoingTaskPullReplicationAsSink = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsSink;
import OngoingTaskPullReplicationAsHub = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskPullReplicationAsHub;
import EtlTaskProgress = Raven.Server.Documents.ETL.Stats.EtlTaskProgress;
import EtlProcessProgress = Raven.Server.Documents.ETL.Stats.EtlProcessProgress;
import TaskUtils from "../../../../utils/TaskUtils";
import { produce, Draft } from "immer";
import OngoingTaskSubscription = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskSubscription;
import OngoingTaskQueueEtlListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueEtl;
import OngoingTaskQueueSinkListView = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskQueueSink;
import OngoingTaskBackup = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskBackup;
import SubscriptionConnectionsDetails = Raven.Server.Documents.TcpHandlers.SubscriptionConnectionsDetails;
import DatabaseUtils from "components/utils/DatabaseUtils";
import { DatabaseSharedInfo } from "components/models/databases";
import ReplicationTaskProgress = Raven.Server.Documents.Replication.Stats.ReplicationTaskProgress;
import ReplicationProcessProgress = Raven.Server.Documents.Replication.Stats.ReplicationProcessProgress;

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

interface ActionTasksLoadError {
    type: "TasksLoadError";
    location: databaseLocationSpecifier;
    error: JQueryXHR;
}

export interface OngoingTasksState {
    tasks: OngoingTaskInfo[];
    subscriptions: OngoingTaskSubscriptionInfo[];
    locations: databaseLocationSpecifier[];
    orchestrators: string[];
    replicationHubs: OngoingTaskHubDefinitionInfo[];
    subscriptionConnectionDetails: SubscriptionConnectionsDetailsWithId[];
}

export type SubscriptionConnectionsDetailsWithId = SubscriptionConnectionsDetails & {
    SubscriptionId: number;
    LoadError?: string;
};

type OngoingTaskReducerAction =
    | ActionTasksLoaded
    | ActionEtlProgressLoaded
    | ActionReplicationProgressLoaded
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
    // TODO: some to process are missing: NumberOfAttachmentTombstones, NumberOfRevisionTombstones, but available in total

    const totalItems =
        taskProgress.TotalNumberOfAttachments +
        // TODO: ?? taskProgress.TotalNumberOfAttachmentTombstones +
        taskProgress.TotalNumberOfCounterGroups +
        taskProgress.TotalNumberOfDocuments +
        taskProgress.TotalNumberOfDocumentTombstones +
        taskProgress.TotalNumberOfRevisions +
        //TODO ?? taskProgress.TotalNumberOfRevisionTombstones +
        taskProgress.TotalNumberOfTimeSeriesDeletedRanges +
        taskProgress.TotalNumberOfTimeSeriesSegments;

    return {
        completed: taskProgress.Completed,
        global: {
            processed:
                totalItems -
                taskProgress.NumberOfAttachmentsToProcess -
                //TODO: ?? TotalNumberOfAttachmentTombstones
                taskProgress.NumberOfCounterGroupsToProcess -
                taskProgress.NumberOfDocumentsToProcess -
                taskProgress.NumberOfDocumentTombstonesToProcess -
                taskProgress.NumberOfRevisionsToProcess -
                //TODO: ?? TotalNumberOfRevisionTombstones
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
        //TODO: other types: attachments, attachment tombstones, revisions, revision tombstones, time series deleted ranges, time series
    };

    // TODO: some to process are missing: NumberOfAttachmentTombstones, NumberOfRevisionTombstones

    /* TODO
    AverageProcessedPerSecond: number;
		DestinationChangeVector: string;
		SourceChangeVector: string;
		FromToString: string;
		LastEtagSent: number;		
     */
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
                fromToString: incoming.FromToString,
                lastAcceptedChangeVectorFromDestination: incoming.LastAcceptedChangeVectorFromDestination,
                lastSentEtag: incoming.LastSentEtag,
                sourceDatabaseChangeVector: incoming.SourceDatabaseChangeVector,
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
                case "AzureQueueStorage": {
                    // noinspection UnnecessaryLocalVariableJS
                    const result: OngoingTaskAzureQueueStorageEtlSharedInfo = {
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

function initNodesInfo(
    taskType: StudioTaskType,
    locations: databaseLocationSpecifier[],
    orchestrators: string[]
): OngoingTaskNodeInfo[] {
    const sharded = orchestrators.length > 0;
    if (sharded && taskType === "Subscription") {
        return orchestrators.map((nodeTag) => ({
            location: {
                nodeTag,
            },
            status: "idle",
            details: null,
        }));
    }

    return locations.map((l) => ({
        location: l,
        status: "idle",
        details: null,
    }));
}

const mapTask = (incomingTask: OngoingTask, incomingLocation: databaseLocationSpecifier, state: OngoingTasksState) => {
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

    return {
        shared: mapSharedInfo(incomingTask),
        nodesInfo: [
            ...nodesInfo.map((x) => (databaseLocationComparator(x.location, newNodeInfo.location) ? newNodeInfo : x)),
        ],
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
                                // The task is already in the state so we update the node info
                                draft.tasks[draftTaskIdx].nodesInfo[draftNodeInfoIdx] = task.nodesInfo[nodeInfoIdx];
                            }
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
                    ) as Draft<OngoingEtlTaskNodeInfo>;

                    nodeInfo.status = "failure";
                    nodeInfo.details = {
                        error: error.responseJSON.Message,
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
            const incomingProgress = action.progress;
            const incomingLocation = action.location;

            return produce(state, (draft) => {
                draft.tasks.forEach((task) => {
                    const perLocationDraft = task.nodesInfo.find((x) =>
                        databaseLocationComparator(x.location, incomingLocation)
                    );

                    //TODO: handle types different then external replication!
                    //TODO for hub we don't have unique anchor

                    const progressToApply = incomingProgress.find(
                        (x) =>
                            (x.ReplicationType === "External" && x.TaskName === task.shared.taskName) ||
                            (x.ReplicationType === "PullAsHub" && x.TaskName === task.shared.taskName) //TODO: for hub it isn't unique selector!
                    );

                    (perLocationDraft as Draft<OngoingReplicationProgressAwareTaskNodeInfo>).progress = progressToApply
                        ? progressToApply.ProcessesProgress.map(mapReplicationProgress)
                        : null;
                });
            });
        }
    }

    return state;
};

export const ongoingTasksReducerInitializer = (db: DatabaseSharedInfo): OngoingTasksState => {
    const locations = DatabaseUtils.getLocations(db);
    const orchestrators = db.isSharded ? db.nodes.map((x) => x.tag) : [];

    return {
        tasks: [],
        subscriptions: [],
        replicationHubs: [],
        locations,
        orchestrators,
        subscriptionConnectionDetails: [],
    };
};
