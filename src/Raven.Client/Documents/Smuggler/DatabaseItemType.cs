using System;

namespace Raven.Client.Documents.Smuggler
{
    [Flags]
    public enum DatabaseItemType
    {
        None = 0,

        Documents = 1 << 0,
        RevisionDocuments = 1 << 1,
        Indexes = 1 << 2,
        Identities = 1 << 4,
        Tombstones = 1 << 5,
        LegacyAttachments = 1 << 6,
        Conflicts = 1 << 7,
        CompareExchange = 1 << 8,
        LegacyDocumentDeletions = 1 << 9,
        LegacyAttachmentDeletions = 1 << 10,
        DatabaseRecord = 1 << 11,
        Unknown = 1 << 12,
        Attachments = 1 << 14,
        CounterGroups = 1 << 15,
        Subscriptions = 1 << 16,
        CompareExchangeTombstones = 1 << 17,
        TimeSeries = 1 << 18,
        ReplicationHubCertificates = 1 << 19,
        TimeSeriesDeletedRanges = 1 << 20
    }

    [Flags]
    public enum DatabaseRecordItemType
    {
        None = 0,

        ConflictSolverConfig = 1 << 0,
        Settings = 1 << 1,
        Revisions = 1 << 2,
        Expiration = 1 << 4,
        PeriodicBackups = 1 << 5,
        ExternalReplications = 1 << 6,
        RavenConnectionStrings = 1 << 7,
        SqlConnectionStrings = 1 << 8,
        RavenEtls = 1 << 9,
        SqlEtls = 1 << 10,
        Client = 1 << 11,
        Sorters = 1 << 12,
        SinkPullReplications = 1 << 13,
        HubPullReplications = 1 << 14,
        TimeSeries = 1 << 15,
        DocumentsCompression = 1 << 16,
        Analyzers = 1 << 17,
        LockMode = 1 << 18,
        OlapConnectionStrings = 1 << 19,
        OlapEtls = 1 << 20,
        ElasticSearchConnectionStrings = 1 << 21,
        ElasticSearchEtls = 1 << 22,
        PostgreSQLIntegration = 1 << 23,
        QueueConnectionStrings = 1 << 24,
        QueueEtls = 1 << 25,
        IndexesHistory = 1 << 26,
        Refresh = 1 << 27,
        QueueSinks = 1 << 28,
        DataArchival = 1 << 29,
        RetireAttachments = 1 << 30,
    }
}
