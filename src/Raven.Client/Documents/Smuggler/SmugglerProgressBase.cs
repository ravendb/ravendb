using System;
using System.Text;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler;

public abstract class SmugglerProgressBase : IOperationProgress
{
    public DatabaseRecordProgress DatabaseRecord { get; set; }

    public CountsWithSkippedCountAndLastEtagAndAttachments Documents { get; set; }

    public CountsWithSkippedCountAndLastEtagAndAttachments RevisionDocuments { get; set; }

    public CountsWithLastEtag Tombstones { get; set; }

    public CountsWithLastEtag Conflicts { get; set; }

    public CountsWithLastEtag Identities { get; set; }

    public Counts Indexes { get; set; }

    public CountsWithLastEtag CompareExchange { get; set; }

    public Counts Subscriptions { get; set; }

    public Counts ReplicationHubCertificates { get; set; }

    public CountsWithSkippedCountAndLastEtag Counters { get; set; }

    public CountsWithSkippedCountAndLastEtag TimeSeries { get; set; }

    public Counts CompareExchangeTombstones { get; set; }

    public CountsWithSkippedCountAndLastEtag TimeSeriesDeletedRanges { get; set; }

    public virtual DynamicJsonValue ToJson()
    {
        return new DynamicJsonValue(GetType())
        {
            [nameof(DatabaseRecord)] = DatabaseRecord.ToJson(),
            [nameof(Documents)] = Documents.ToJson(),
            [nameof(RevisionDocuments)] = RevisionDocuments.ToJson(),
            [nameof(Tombstones)] = Tombstones.ToJson(),
            [nameof(Conflicts)] = Conflicts.ToJson(),
            [nameof(Identities)] = Identities.ToJson(),
            [nameof(Indexes)] = Indexes.ToJson(),
            [nameof(CompareExchange)] = CompareExchange.ToJson(),
            [nameof(Subscriptions)] = Subscriptions.ToJson(),
            [nameof(Counters)] = Counters.ToJson(),
            [nameof(CompareExchangeTombstones)] = CompareExchangeTombstones.ToJson(),
            [nameof(TimeSeries)] = TimeSeries.ToJson(),
            [nameof(ReplicationHubCertificates)] = ReplicationHubCertificates.ToJson(),
            [nameof(TimeSeriesDeletedRanges)] = TimeSeriesDeletedRanges.ToJson()
        };
    }

    public IOperationProgress Clone()
    {
        throw new NotSupportedException();
    }

    public bool CanMerge => false;

    public void MergeWith(IOperationProgress progress)
    {
        throw new NotSupportedException();
    }

    public sealed class DatabaseRecordProgress : Counts
    {
        public bool SortersUpdated { get; set; }

        public bool AnalyzersUpdated { get; set; }

        public bool SinkPullReplicationsUpdated { get; set; }

        public bool HubPullReplicationsUpdated { get; set; }

        public bool RavenEtlsUpdated { get; set; }

        public bool SqlEtlsUpdated { get; set; }

        public bool ExternalReplicationsUpdated { get; set; }

        public bool PeriodicBackupsUpdated { get; set; }

        public bool ConflictSolverConfigUpdated { get; set; }

        public bool TimeSeriesConfigurationUpdated { get; set; }

        public bool DocumentsCompressionConfigurationUpdated { get; set; }

        public bool RevisionsConfigurationUpdated { get; set; }

        public bool ExpirationConfigurationUpdated { get; set; }

        public bool RefreshConfigurationUpdated { get; set; }
        
        public bool DataArchivalConfigurationUpdated { get; set; }

        public bool RetireAttachmentsConfigurationUpdated { get; set; }

        public bool RavenConnectionStringsUpdated { get; set; }

        public bool SqlConnectionStringsUpdated { get; set; }

        public bool ClientConfigurationUpdated { get; set; }

        public bool UnusedDatabaseIdsUpdated { get; set; }

        public bool LockModeUpdated { get; set; }

        public bool OlapEtlsUpdated { get; set; }

        public bool OlapConnectionStringsUpdated { get; set; }

        public bool ElasticSearchEtlsUpdated { get; set; }

        public bool ElasticSearchConnectionStringsUpdated { get; set; }

        public bool PostreSQLConfigurationUpdated { get; set; }

        public bool QueueEtlsUpdated { get; set; }

        public bool QueueConnectionStringsUpdated { get; set; }

        public bool QueueSinksUpdated { get; set; }

        public bool IndexesHistoryUpdated { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();

            if (TimeSeriesConfigurationUpdated)
                json[nameof(TimeSeriesConfigurationUpdated)] = TimeSeriesConfigurationUpdated;

            if (DocumentsCompressionConfigurationUpdated)
                json[nameof(DocumentsCompressionConfigurationUpdated)] = DocumentsCompressionConfigurationUpdated;

            if (RevisionsConfigurationUpdated)
                json[nameof(RevisionsConfigurationUpdated)] = RevisionsConfigurationUpdated;

            if (ExpirationConfigurationUpdated)
                json[nameof(ExpirationConfigurationUpdated)] = ExpirationConfigurationUpdated;

            if (RefreshConfigurationUpdated)
                json[nameof(RefreshConfigurationUpdated)] = RefreshConfigurationUpdated;
            
            if (DataArchivalConfigurationUpdated)
                json[nameof(DataArchivalConfigurationUpdated)] = DataArchivalConfigurationUpdated;

            if (RetireAttachmentsConfigurationUpdated)
                json[nameof(RetireAttachmentsConfigurationUpdated)] = RetireAttachmentsConfigurationUpdated;

            if (RavenConnectionStringsUpdated)
                json[nameof(RavenConnectionStringsUpdated)] = RavenConnectionStringsUpdated;

            if (SqlConnectionStringsUpdated)
                json[nameof(SqlConnectionStringsUpdated)] = SqlConnectionStringsUpdated;

            if (ClientConfigurationUpdated)
                json[nameof(ClientConfigurationUpdated)] = ClientConfigurationUpdated;

            if (ConflictSolverConfigUpdated)
                json[nameof(ConflictSolverConfigUpdated)] = ConflictSolverConfigUpdated;

            if (PeriodicBackupsUpdated)
                json[nameof(PeriodicBackupsUpdated)] = PeriodicBackupsUpdated;

            if (ExternalReplicationsUpdated)
                json[nameof(ExternalReplicationsUpdated)] = ExternalReplicationsUpdated;

            if (SqlEtlsUpdated)
                json[nameof(SqlEtlsUpdated)] = SqlEtlsUpdated;

            if (RavenEtlsUpdated)
                json[nameof(RavenEtlsUpdated)] = RavenEtlsUpdated;

            if (SortersUpdated)
                json[nameof(SortersUpdated)] = SortersUpdated;

            if (AnalyzersUpdated)
                json[nameof(AnalyzersUpdated)] = AnalyzersUpdated;

            if (SinkPullReplicationsUpdated)
                json[nameof(SinkPullReplicationsUpdated)] = SinkPullReplicationsUpdated;

            if (HubPullReplicationsUpdated)
                json[nameof(HubPullReplicationsUpdated)] = HubPullReplicationsUpdated;

            if (UnusedDatabaseIdsUpdated)
                json[nameof(UnusedDatabaseIdsUpdated)] = UnusedDatabaseIdsUpdated;

            if (LockModeUpdated)
                json[nameof(LockModeUpdated)] = LockModeUpdated;

            if (OlapConnectionStringsUpdated)
                json[nameof(OlapConnectionStringsUpdated)] = OlapConnectionStringsUpdated;

            if (OlapEtlsUpdated)
                json[nameof(OlapEtlsUpdated)] = OlapEtlsUpdated;

            if (ElasticSearchConnectionStringsUpdated)
                json[nameof(ElasticSearchConnectionStringsUpdated)] = ElasticSearchConnectionStringsUpdated;

            if (ElasticSearchEtlsUpdated)
                json[nameof(ElasticSearchEtlsUpdated)] = ElasticSearchEtlsUpdated;

            if (QueueConnectionStringsUpdated)
                json[nameof(QueueConnectionStringsUpdated)] = QueueConnectionStringsUpdated;

            if (QueueEtlsUpdated)
                json[nameof(QueueEtlsUpdated)] = QueueEtlsUpdated;

            if (PostreSQLConfigurationUpdated)
                json[nameof(PostreSQLConfigurationUpdated)] = PostreSQLConfigurationUpdated;

            if (IndexesHistoryUpdated)
                json[nameof(IndexesHistoryUpdated)] = IndexesHistoryUpdated;

            if (QueueSinksUpdated)
                json[nameof(QueueSinksUpdated)] = QueueSinksUpdated;

            return json;
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            if (RevisionsConfigurationUpdated)
                sb.AppendLine("- Revisions");

            if (ExpirationConfigurationUpdated)
                sb.AppendLine("- Expiration");

            if (RefreshConfigurationUpdated)
                sb.AppendLine("- Refresh");
            
            if (DataArchivalConfigurationUpdated)
                sb.AppendLine("- Data Archival");

            if (RetireAttachmentsConfigurationUpdated)
                sb.AppendLine("- Retire Attachments");

            if (RavenConnectionStringsUpdated)
                sb.AppendLine("- RavenDB Connection Strings");

            if (SqlConnectionStringsUpdated)
                sb.AppendLine("- SQL Connection Strings");

            if (ConflictSolverConfigUpdated)
                sb.AppendLine("- Conflicts Solvers");

            if (PeriodicBackupsUpdated)
                sb.AppendLine("- Periodic Backups");

            if (ExternalReplicationsUpdated)
                sb.AppendLine("- External Replications");

            if (RavenEtlsUpdated)
                sb.AppendLine("- RavenDB ETLs");

            if (SqlEtlsUpdated)
                sb.AppendLine("- SQL ETLs");

            if (SortersUpdated)
                sb.AppendLine("- Sorters");

            if (AnalyzersUpdated)
                sb.AppendLine("- Analyzers");

            if (SinkPullReplicationsUpdated)
                sb.AppendLine("- Pull Replication Sinks");

            if (HubPullReplicationsUpdated)
                sb.AppendLine("- Pull Replication Hubs");

            if (ClientConfigurationUpdated)
                sb.AppendLine("- Client");

            if (UnusedDatabaseIdsUpdated)
                sb.AppendLine("- Unused Database IDs");

            if (TimeSeriesConfigurationUpdated)
                sb.AppendLine("- Time Series");

            if (DocumentsCompressionConfigurationUpdated)
                sb.AppendLine("- Documents Compression");

            if (LockModeUpdated)
                sb.AppendLine("- Lock Mode");

            if (OlapConnectionStringsUpdated)
                sb.AppendLine("- OLAP Connection Strings");

            if (OlapEtlsUpdated)
                sb.AppendLine("- OLAP ETLs");

            if (ElasticSearchConnectionStringsUpdated)
                sb.AppendLine("- ElasticSearch Connection Strings");

            if (ElasticSearchEtlsUpdated)
                sb.AppendLine("- ElasticSearch ETLs");

            if (QueueConnectionStringsUpdated)
                sb.AppendLine("- Queue Connection Strings");

            if (QueueEtlsUpdated)
                sb.AppendLine("- Queue ETLs");

            if (PostreSQLConfigurationUpdated)
                sb.AppendLine("- PostgreSQL Integration");

            if (IndexesHistoryUpdated)
                sb.AppendLine("- Indexes History");

            if (sb.Length == 0)
                return string.Empty;

            sb.Insert(0, "Following configurations were updated:" + Environment.NewLine);

            return sb.ToString();
        }
    }

    public sealed class FileCounts
    {
        public string CurrentFileName { get; set; }
        public long CurrentFile { get; set; }
        public long FileCount { get; set; }

        public DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(CurrentFileName)] = CurrentFileName,
                [nameof(CurrentFile)] = CurrentFile,
                [nameof(FileCount)] = FileCount
            };
        }
    }

    public class Counts
    {
        public DateTime? StartTime { get; set; }
        public bool Processed { get; set; }
        public long ReadCount { get; set; }
        public bool Skipped { get; set; }
        public long ErroredCount { get; set; }
        public long SizeInBytes { get; set; }

        public virtual DynamicJsonValue ToJson()
        {
            return new DynamicJsonValue
            {
                [nameof(StartTime)] = StartTime,
                [nameof(Processed)] = Processed,
                [nameof(ReadCount)] = ReadCount,
                [nameof(Skipped)] = Skipped,
                [nameof(ErroredCount)] = ErroredCount,
                [nameof(SizeInBytes)] = SizeInBytes
            };
        }

        public override string ToString()
        {
            var sb = new StringBuilder();
            sb.Append($"Read: {ReadCount:#,#;;0}.");
            if (ErroredCount > 0)
                sb.Append($" Errored: {ErroredCount:#,#;;0}.");
            if (SizeInBytes > 0)
                sb.Append($" Size: {new Size(SizeInBytes).HumaneSize}.");

            return sb.ToString();
        }

        internal void Start()
        {
            StartTime ??= SystemTime.UtcNow;
        }
    }

    public class CountsWithLastEtag : Counts
    {
        public long LastEtag { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(LastEtag)] = LastEtag;
            return json;
        }
    }

    public class CountsWithLastEtagAndAttachments : CountsWithLastEtag
    {
        public Counts Attachments { get; set; } = new Counts();

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(Attachments)] = Attachments.ToJson();
            return json;
        }

        public override string ToString()
        {
            if (Attachments is { ReadCount: > 0 })
            {
                return $"{base.ToString()}" +
                       $"{Environment.NewLine}Attachments: {Attachments}";
            }

            return base.ToString();
        }
    }

    public sealed class CountsWithSkippedCountAndLastEtag : CountsWithLastEtag
    {
        public long SkippedCount { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SkippedCount)] = SkippedCount;
            return json;
        }

        public override string ToString()
        {
            return $"Skipped: {SkippedCount}. {base.ToString()}";
        }
    }

    public sealed class CountsWithSkippedCountAndLastEtagAndAttachments : CountsWithLastEtagAndAttachments
    {
        public long SkippedCount { get; set; }

        public override DynamicJsonValue ToJson()
        {
            var json = base.ToJson();
            json[nameof(SkippedCount)] = SkippedCount;
            return json;
        }

        public override string ToString()
        {
            if (SkippedCount > 0)
                return $"Skipped: {SkippedCount}. {base.ToString()}";

            return base.ToString();
        }
    }
}
