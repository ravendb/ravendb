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

        [Obsolete("DatabaseItemType.Counters is not supported anymore. Will be removed in next major version of the product.")]
        Counters = 1 << 13,

        Attachments = 1 << 14,
        CounterGroups = 1 << 15,
        Subscriptions = 1 << 16,
        CompareExchangeTombstones = 1 << 17,
        TimeSeries = 1 << 18,

        ReplicationHubCertificates = 1 << 19
    }

    [Flags]
    public enum DatabaseRecordItemType
    {
        None = 0,

        ConflictSolverConfig = 1 << 0,
        Settings = 1 << 1,
        Revisions = 1 << 2,
        Expiration = 1 << 3,
        PeriodicBackups = 1 << 4,
        ExternalReplications = 1 << 5,
        RavenConnectionStrings = 1 << 6,
        SqlConnectionStrings = 1 << 7,
        OlapConnectionStrings = 1 << 8,
        RavenEtls = 1 << 9,
        SqlEtls = 1 << 10,
        OlapEtls = 1 << 11,
        Client = 1 << 12,
        Sorters = 1 << 13,
        SinkPullReplications = 1 << 14,
        HubPullReplications = 1 << 15,
        TimeSeries = 1 << 16,
        DocumentsCompression = 1 << 17,
        Analyzers = 1 << 18,
        LockMode = 1 << 19
    }
}
