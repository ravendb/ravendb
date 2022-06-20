import { Reducer } from "react";
import {
    AnyEtlOngoingTaskInfo,
    OngoingEtlTaskNodeInfo,
    OngoingTaskElasticSearchEtlSharedInfo,
    OngoingTaskExternalReplicationSharedInfo,
    OngoingTaskHubDefinitionInfo,
    OngoingTaskInfo,
    OngoingTaskNodeInfo,
    OngoingTaskNodeInfoDetails,
    OngoingTaskNodeProgressDetails,
    OngoingTaskOlapEtlSharedInfo,
    OngoingTaskRavenEtlSharedInfo,
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskReplicationSinkSharedInfo,
    OngoingTaskSharedInfo,
    OngoingTaskSqlEtlSharedInfo,
    OngoingTaskSubscriptionInfo,
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

interface OngoingTasksState {
    tasks: OngoingTaskInfo[];
    locations: databaseLocationSpecifier[];
    replicationHubs: OngoingTaskHubDefinitionInfo[];
}

type OngoingTaskReducerAction = ActionTasksLoaded | ActionProgressLoaded;

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
        taskType,
        taskName: task.TaskName,
        taskId: task.TaskId,
        mentorName: task.MentorNode,
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

        //TODO: backup
    }

    return commonProps;
}

function mapNodeInfo(task: OngoingTask): OngoingTaskNodeInfoDetails {
    return {
        taskConnectionStatus: task.TaskConnectionStatus,
        responsibleNode: task.ResponsibleNode?.NodeTag,
        error: task.Error,
    };
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
                    const existingTask = state.tasks.find(
                        (x) => x.shared.taskType === incomingTask.TaskType && x.shared.taskId === incomingTask.TaskId
                    );
                    const nodesInfo = existingTask ? existingTask.nodesInfo : initNodesInfo(state.locations);
                    const newNodeInfo: OngoingTaskNodeInfo = {
                        location: incomingLocation,
                        status: "loaded",
                        details: mapNodeInfo(incomingTask),
                    };

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
                            serverWide: incomingTask.Name.startsWith(serverWidePrefix),
                            taskType: "PullReplicationAsHub",
                            mentorName: null,
                        },
                        nodesInfo: undefined,
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
                            TaskUtils.etlTypeToTaskType(x.EtlType) === task.shared.taskType &&
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
