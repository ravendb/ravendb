import { loadStatus } from "./common";
import OngoingTaskState = Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskState;
import BackupType = Raven.Client.Documents.Operations.Backups.BackupType;
import PullReplicationMode = Raven.Client.Documents.Operations.Replication.PullReplicationMode;

export interface OngoingTaskHubDefinitionSharedInfo extends OngoingTaskSharedInfo {
    delayReplicationTime: number;
    taskMode: PullReplicationMode;
    hasFiltering: boolean;
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
    transactionalId?: string;
}

export interface OngoingTaskNodeInfoDetails {
    taskConnectionStatus: Raven.Client.Documents.Operations.OngoingTasks.OngoingTaskConnectionStatus;
    responsibleNode: string;
    error: string;
}

export interface OngoingTaskNodeInfo<TNodeInfo extends OngoingTaskNodeInfoDetails = OngoingTaskNodeInfoDetails> {
    location: databaseLocationSpecifier;
    status: loadStatus;
    details: TNodeInfo;
}

export interface OngoingEtlTaskNodeInfo<TNodeInfo extends OngoingTaskNodeInfoDetails = OngoingTaskNodeInfoDetails>
    extends OngoingTaskNodeInfo<TNodeInfo> {
    etlProgress: OngoingTaskNodeProgressDetails[];
}

export type OngoingSubscriptionTaskNodeInfo = OngoingTaskNodeInfo<OngoingTaskSubscriptionNodeInfoDetails>;

export interface OngoingTaskSharedInfo {
    taskName: string;
    taskId: number;
    taskType: StudioTaskType;
    mentorNodeTag: string;
    responsibleNodeTag: string;
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
    backupDestinations: string[];
    lastExecutingNodeTag: string;
    lastFullBackup: string;
    lastIncrementalBackup: string;
    backupType: BackupType;
    encrypted: boolean;
    nextBackup: Raven.Client.Documents.Operations.OngoingTasks.NextBackup;
    retentionPolicy: Raven.Client.Documents.Operations.Backups.RetentionPolicy;
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

export interface OngoingTaskQueueSinkSharedInfo extends OngoingTaskSharedInfo {
    connectionStringName: string;
    url: string;
}

export type OngoingTaskKafkaEtlSharedInfo = OngoingTaskQueueEtlSharedInfo;

export type OngoingTaskRabbitMqEtlSharedInfo = OngoingTaskQueueEtlSharedInfo;
export type OngoingTaskAzureQueueStorageEtlSharedInfo = OngoingTaskQueueEtlSharedInfo;

export type OngoingTaskKafkaSinkSharedInfo = OngoingTaskQueueSinkSharedInfo;

export type OngoingTaskRabbitMqSinkSharedInfo = OngoingTaskQueueSinkSharedInfo;

export interface OngoingTaskQueueEtlSharedInfo extends OngoingTaskSharedInfo {
    connectionStringName: string;
    url: string;
}

export interface OngoingTaskSqlEtlSharedInfo extends OngoingTaskSharedInfo {
    destinationServer: string;
    destinationDatabase: string;
    connectionStringName: string;
    connectionStringDefined: boolean;
}

export interface OngoingTaskSnowflakeEtlSharedInfo extends OngoingTaskSharedInfo {
    connectionStringName: string;
}

export interface OngoingTaskSubscriptionSharedInfo extends OngoingTaskSharedInfo {
    changeVectorForNextBatchStartingPoint: string;
    changeVectorForNextBatchStartingPointPerShard: { [key: string]: string };
    lastBatchAckTime?: string;
    lastClientConnectionTime?: string;
}

export type OngoingTaskElasticSearchEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskExternalReplicationNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskOlapEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export interface OngoingTaskPeriodicBackupNodeInfoDetails extends OngoingTaskNodeInfoDetails {
    onGoingBackup: Raven.Client.Documents.Operations.OngoingTasks.RunningBackup;
}

export type OngoingTaskRavenEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskReplicationHubNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskReplicationSinkNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskSqlEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskSnowflakeEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskSubscriptionNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskKafkaEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskRabbitMqEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskAzureQueueStorageEtlNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskKafkaSinkNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type OngoingTaskRabbitMqSinkNodeInfoDetails = OngoingTaskNodeInfoDetails;

export type AnyEtlOngoingTaskInfo =
    | OngoingTaskSqlEtlInfo
    | OngoingTaskSnowflakeEtlInfo
    | OngoingTaskOlapEtlInfo
    | OngoingTaskElasticSearchEtlInfo
    | OngoingTaskRavenEtlInfo
    | OngoingTaskKafkaEtlInfo
    | OngoingTaskRabbitMqEtlInfo
    | OngoingTaskAzureQueueStorageEtlInfo;

export interface OngoingTaskInfo<
    TSharded extends OngoingTaskSharedInfo = OngoingTaskSharedInfo,
    TNodesInfo extends OngoingTaskNodeInfo = OngoingTaskNodeInfo,
> {
    shared: TSharded;
    nodesInfo: TNodesInfo[];
}

export type OngoingTaskElasticSearchEtlInfo = OngoingTaskInfo<
    OngoingTaskElasticSearchEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskElasticSearchEtlNodeInfoDetails>
>;

export type OngoingTaskExternalReplicationInfo = OngoingTaskInfo<
    OngoingTaskExternalReplicationSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskExternalReplicationNodeInfoDetails>
>;

export type OngoingTaskOlapEtlInfo = OngoingTaskInfo<
    OngoingTaskOlapEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskOlapEtlNodeInfoDetails>
>;

export type OngoingTaskPeriodicBackupInfo = OngoingTaskInfo<
    OngoingTaskPeriodicBackupSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskPeriodicBackupNodeInfoDetails>
>;

export type OngoingTaskRavenEtlInfo = OngoingTaskInfo<
    OngoingTaskRavenEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskRavenEtlNodeInfoDetails>
>;

export type OngoingTaskReplicationHubInfo = OngoingTaskInfo<
    OngoingTaskReplicationHubSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskReplicationHubNodeInfoDetails>
>;

export type OngoingTaskHubDefinitionInfo = OngoingTaskInfo<OngoingTaskHubDefinitionSharedInfo, never>;

export type OngoingTaskReplicationSinkInfo = OngoingTaskInfo<
    OngoingTaskReplicationSinkSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskReplicationSinkNodeInfoDetails>
>;

export type OngoingTaskSqlEtlInfo = OngoingTaskInfo<
    OngoingTaskSqlEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskSqlEtlNodeInfoDetails>
>;

export type OngoingTaskSnowflakeEtlInfo = OngoingTaskInfo<
    OngoingTaskSnowflakeEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskSnowflakeEtlNodeInfoDetails>
>;

export type OngoingTaskKafkaEtlInfo = OngoingTaskInfo<
    OngoingTaskKafkaEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskKafkaEtlNodeInfoDetails>
>;

export type OngoingTaskRabbitMqEtlInfo = OngoingTaskInfo<
    OngoingTaskRabbitMqEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskRabbitMqEtlNodeInfoDetails>
>;

export type OngoingTaskAzureQueueStorageEtlInfo = OngoingTaskInfo<
    OngoingTaskAzureQueueStorageEtlSharedInfo,
    OngoingEtlTaskNodeInfo<OngoingTaskAzureQueueStorageEtlNodeInfoDetails>
>;

export type OngoingTaskKafkaSinkInfo = OngoingTaskInfo<
    OngoingTaskKafkaSinkSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskKafkaSinkNodeInfoDetails>
>;

export type OngoingTaskRabbitMqSinkInfo = OngoingTaskInfo<
    OngoingTaskRabbitMqSinkSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskRabbitMqSinkNodeInfoDetails>
>;

export type OngoingTaskSubscriptionInfo = OngoingTaskInfo<
    OngoingTaskSubscriptionSharedInfo,
    OngoingTaskNodeInfo<OngoingTaskSubscriptionNodeInfoDetails>
>;
