import { loadStatus } from "./common";
import OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;

export interface OngoingTaskHubDefinitionSharedInfo extends OngoingTaskSharedInfo {
    delayReplicationTime: number;
}

interface Progress {
    total: number;
    processed: number;
}

export interface OngoingTaskNodeProgressDetails {
    global: Progress;
    documents: Progress;
    documentTombstones: Progress;
    counterGroups: Progress;
    transformationName: string;
    disabled: boolean;
    completed: boolean;
    processedPerSecond: number;
}

export interface OngoingTaskNodeInfoDetails {
    taskConnectionStatus: OngoingTaskConnectionStatus;
    responsibleNode: string;
    error: string;
}

export interface OngoingTaskNodeInfo<TNodeInfo extends OngoingTaskNodeInfoDetails = OngoingTaskNodeInfoDetails> {
    location: databaseLocationSpecifier;
    status: loadStatus;
    details: TNodeInfo;
    progress: OngoingTaskNodeProgressDetails[];
}

export interface OngoingTaskSharedInfo {
    taskName: string;
    taskId: number;
    taskType: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskType;
    mentorName: string;
    taskState: OngoingTaskState;
    serverWide: boolean;
}

export interface OngoingTaskElasticSearchEtlSharedInfo extends OngoingTaskSharedInfo {
    connectionStringName: string;
    nodesUrls: string[];
}

export interface OngoingTaskExternalReplicationSharedInfo extends OngoingTaskSharedInfo {
    destinationDatabase: string;
    destinationUrl: string;
    connectionStringName: string;
    topologyDiscoveryUrls: string[];
    delayReplicationTime: number;
}

export interface OngoingTaskOlapEtlSharedInfo extends OngoingTaskSharedInfo {
    connectionStringName: string;
    destinationDescription: string;
    destinations: string[];
}

export interface OngoingTaskPeriodicBackupSharedInfo extends OngoingTaskSharedInfo {
    //TODO:
}

export interface OngoingTaskRavenEtlSharedInfo extends OngoingTaskSharedInfo {
    destinationDatabase: string;
    destinationUrl: string;
    connectionStringName: string;
    topologyDiscoveryUrls: string[];
}

export interface OngoingTaskReplicationHubSharedInfo extends OngoingTaskSharedInfo {
    destinationDatabase: string;
    destinationUrl: string;
}

export interface OngoingTaskReplicationSinkSharedInfo extends OngoingTaskSharedInfo {
    destinationDatabase: string;
    destinationUrl: string;
    connectionStringName: string;
    topologyDiscoveryUrls: string[];
    hubName: string;
    mode: PullReplicationMode;
}

export interface OngoingTaskSqlEtlSharedInfo extends OngoingTaskSharedInfo {
    destinationServer: string;
    destinationDatabase: string;
    connectionStringName: string;
    connectionStringDefined: boolean;
}

export interface OngoingTaskSubscriptionSharedInfo extends OngoingTaskSharedInfo {
    //TODO:
}

export interface OngoingTaskElasticSearchEtlNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskExternalReplicationNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskOlapEtlNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskPeriodicBackupNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO: LastExecutingNodeTag - is local?
}

export interface OngoingTaskRavenEtlNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskReplicationHubNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskReplicationSinkNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskSqlEtlNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO:
}

export interface OngoingTaskSubscriptionNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    //TODO: ChangeVectorForNextBatchStartingPoint and ChangeVectorForNextBatchStartingPointPerShard
}

export interface OngoingTaskInfo<
    TSharded extends OngoingTaskSharedInfo = OngoingTaskSharedInfo,
    TNodeInfo extends OngoingTaskNodeInfoDetails = OngoingTaskNodeInfoDetails
> {
    shared: TSharded;
    nodesInfo: OngoingTaskNodeInfo<TNodeInfo>[];
}

type OngoingTaskElasticSearchEtlInfo = OngoingTaskInfo<
    OngoingTaskElasticSearchEtlSharedInfo,
    OngoingTaskElasticSearchEtlNodeInfoDetails
>;

type OngoingTaskExternalReplicationInfo = OngoingTaskInfo<
    OngoingTaskExternalReplicationSharedInfo,
    OngoingTaskExternalReplicationNodeInfoDetails
>;

type OngoingTaskOlapEtlInfo = OngoingTaskInfo<OngoingTaskOlapEtlSharedInfo, OngoingTaskOlapEtlNodeInfoDetails>;

type OngoingTaskPeriodicBackupInfo = OngoingTaskInfo<
    OngoingTaskPeriodicBackupSharedInfo,
    OngoingTaskPeriodicBackupNodeInfoDetails
>;

type OngoingTaskRavenEtlInfo = OngoingTaskInfo<OngoingTaskRavenEtlSharedInfo, OngoingTaskRavenEtlNodeInfoDetails>;

type OngoingTaskReplicationHubInfo = OngoingTaskInfo<
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskReplicationHubNodeInfoDetails
>;

type OngoingTaskHubDefinitionInfo = OngoingTaskInfo<OngoingTaskHubDefinitionSharedInfo, never>;

type OngoingTaskReplicationSinkInfo = OngoingTaskInfo<
    OngoingTaskReplicationSinkSharedInfo,
    OngoingTaskReplicationSinkNodeInfoDetails
>;

type OngoingTaskSqlEtlInfo = OngoingTaskInfo<OngoingTaskSqlEtlSharedInfo, OngoingTaskSqlEtlNodeInfoDetails>;

type OngoingTaskSubscriptionInfo = OngoingTaskInfo<
    OngoingTaskSubscriptionSharedInfo,
    OngoingTaskSubscriptionNodeInfoDetails
>;
