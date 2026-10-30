type INotificationDetails = Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails;
type AlertReason = Raven.Server.NotificationCenter.Notifications.AlertReason;
type PerformanceHintReason = Raven.Server.NotificationCenter.Notifications.PerformanceHintReason;
type RequestLatencyInfo = Raven.Server.NotificationCenter.Notifications.Details.RequestLatencyInfo;

export interface BlockingTombstonesDetails {
    BlockingTombstones: Raven.Server.NotificationCenter.BlockingTombstoneDetails[];
}

export interface RemoteAttachmentErrorInfo {
    Date: string;
    Error: string;
    Identifier: string;
    Hash: string;
    Ids: string[];
}

export interface RemoteAttachmentErrorsDetails {
    Errors: RemoteAttachmentErrorInfo[];
}

export interface RequestLatencyDetails {
    RequestLatencies: Record<string, RequestLatencyInfo[]>;
}

interface AlertDetailsByReason {
    AiAgent_ExceededTokenThreshold: Raven.Server.NotificationCenter.Notifications.Details.ExceededTokenThresholdDetails;
    Attachments_RemoteAttachmentErroredIdentifier: RemoteAttachmentErrorsDetails;
    BlockingTombstones: BlockingTombstonesDetails;
    ConflictRevisionsExceeded: Raven.Server.NotificationCenter.Notifications.Details.ConflictPerformanceDetails;
    Etl_HealthStatusChange: Raven.Server.NotificationCenter.Notifications.Details.EtlTaskHealthChangeDetails;
    Indexing_CoraxComplexItem: Raven.Server.NotificationCenter.Notifications.Details.ComplexFieldsWarning;
    MismatchedReferenceLoad: Raven.Server.NotificationCenter.Notifications.Details.MismatchedReferencesLoadWarning;
    QueueSink_ConsumeError: Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorsDetails;
    QueueSink_ScriptError: Raven.Server.NotificationCenter.Notifications.Details.QueueSinkErrorsDetails;
    Server_NewVersionAvailable: Raven.Server.NotificationCenter.Notifications.Details.NewVersionAvailableDetails;
    ServerLimits: Raven.Server.NotificationCenter.Notifications.Details.ServerLimitsDetails;
    Throttling_CpuCreditsBalance: Raven.Server.NotificationCenter.Notifications.Details.CpuCreditsExhaustionWarning;
}

export type AlertDetails<TReason extends AlertReason> = TReason extends keyof AlertDetailsByReason
    ? AlertDetailsByReason[TReason]
    : INotificationDetails;

interface PerformanceHintDetailsByReason {
    HugeDocuments: Raven.Server.NotificationCenter.Notifications.Details.HugeDocumentsDetails;
    Indexing: Raven.Server.NotificationCenter.Notifications.Details.WarnIndexOutputsPerDocument;
    Indexing_References: Raven.Server.NotificationCenter.Notifications.Details.IndexingReferenceLoadWarning;
    None: null;
    Paging: Raven.Server.NotificationCenter.Notifications.Details.PagingPerformanceDetails;
    Replication: null;
    RequestLatency: RequestLatencyDetails;
    SlowIO: Raven.Server.NotificationCenter.Notifications.Details.SlowIoDetails;
    SqlEtl_SlowSql: Raven.Server.NotificationCenter.Notifications.Details.SlowSqlDetails;
    UnusedCapacity: null;
}

export type PerformanceHintDetails<TReason extends PerformanceHintReason> = PerformanceHintDetailsByReason[TReason];
