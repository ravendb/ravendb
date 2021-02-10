using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using Raven.Client.Documents.Operations;
using Raven.Client.Util;
using Sparrow.Json.Parsing;

namespace Raven.Client.Documents.Smuggler
{
    public class SmugglerResult : SmugglerProgressBase, IOperationResult
    {
        private readonly List<string> _messages;
        protected SmugglerProgress _progress;
        private readonly Stopwatch _sw;

        public SmugglerResult()
        {
            _sw = Stopwatch.StartNew();
            _messages = new List<string>();

            /*
            *  NOTE:
            *
            *  About to add new/change property below?
            *
            *  Please remember to include this property in SmugglerProgress class
            */

            DatabaseRecord = new DatabaseRecordProgress();
            Documents = new CountsWithSkippedCountAndLastEtag();
            RevisionDocuments = new CountsWithLastEtag();
            Tombstones = new CountsWithLastEtag();
            Conflicts = new CountsWithLastEtag();
            Identities = new CountsWithLastEtag();
            Indexes = new Counts();
            CompareExchange = new CountsWithLastEtag();
            Counters = new CountsWithLastEtag();
            CompareExchangeTombstones = new Counts();
            Subscriptions = new Counts();
            _progress = new SmugglerProgress(this);
        }

        public string Message { get; private set; }

        public TimeSpan Elapsed => _sw.Elapsed;

        public IOperationProgress Progress => _progress;

        public IReadOnlyList<string> Messages => _messages;

        public void AddWarning(string message)
        {
            AddMessage("WARNING", message);
        }

        public void AddInfo(string message)
        {
            AddMessage("INFO", message);
        }

        public void AddError(string message)
        {
            AddMessage("ERROR", message);
        }

        internal void AddMessage(string message)
        {
            Message = message;

            lock (this)
            {
                _messages.Add(Message);
            }
        }

        private void AddMessage(string type, string message)
        {
            Message = $"[{SystemTime.UtcNow:T} {type}] {message}";

            lock (this)
            {
                _messages.Add(Message);
            }
        }

        public override DynamicJsonValue ToJson()
        {
            _sw.Stop();

            var json = base.ToJson();

            lock (this)
            {
                json[nameof(Messages)] = Messages.ToList();
            }

            json[nameof(Elapsed)] = Elapsed;

            return json;
        }

        public bool ShouldPersist => true;

        public string LegacyLastDocumentEtag { get; set; }
        public string LegacyLastAttachmentEtag { get; set; }

        public class SmugglerProgress : SmugglerProgressBase, IOperationProgress
        {
            protected readonly SmugglerResult _result;

            public SmugglerProgress()
                : this(null)
            {
                // for deserialization
            }

            public SmugglerProgress(SmugglerResult result)
            {
                _result = result;
                Message = _result?.Message;
                DatabaseRecord = _result?.DatabaseRecord;
                Documents = _result?.Documents;
                RevisionDocuments = _result?.RevisionDocuments;
                Tombstones = _result?.Tombstones;
                Conflicts = _result?.Conflicts;
                Identities = _result?.Identities;
                Indexes = _result?.Indexes;
                CompareExchange = _result?.CompareExchange;
                Counters = _result?.Counters;
                CompareExchangeTombstones = _result?.CompareExchangeTombstones;
                Subscriptions = _result?.Subscriptions;
            }

            private string Message { get; set; }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(Message)] = _result?.Message ?? Message;
                return json;
            }
        }

        public long GetLastEtag()
        {
            var lastEtag = Documents.LastEtag;

            if (RevisionDocuments.LastEtag > lastEtag)
                lastEtag = RevisionDocuments.LastEtag;

            if (Tombstones.LastEtag > lastEtag)
                lastEtag = Tombstones.LastEtag;

            if (Conflicts.LastEtag > lastEtag)
                lastEtag = Conflicts.LastEtag;

            if (Counters.LastEtag > lastEtag)
                lastEtag = Counters.LastEtag;

            return lastEtag;
        }

        public long GetLastRaftIndex()
        {
            var lastEtag = Identities.LastEtag;

            if (CompareExchange.LastEtag > lastEtag)
                lastEtag = CompareExchange.LastEtag;

            return lastEtag;
        }
    }

    public abstract class SmugglerProgressBase
    {
        public DatabaseRecordProgress DatabaseRecord { get; set; }

        public CountsWithSkippedCountAndLastEtag Documents { get; set; }

        public CountsWithLastEtag RevisionDocuments { get; set; }

        public CountsWithLastEtag Tombstones { get; set; }

        public CountsWithLastEtag Conflicts { get; set; }

        public CountsWithLastEtag Identities { get; set; }

        public Counts Indexes { get; set; }

        public CountsWithLastEtag CompareExchange { get; set; }

        public Counts Subscriptions { get; set; }
        public CountsWithLastEtag Counters { get; set; }

        public Counts CompareExchangeTombstones { get; set; }

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
                [nameof(CompareExchangeTombstones)] = CompareExchangeTombstones.ToJson()
            };
        }

        public class DatabaseRecordProgress : Counts
        {
            public bool SortersUpdated { get; set; }

            public bool SinkPullReplicationsUpdated { get; set; }

            public bool HubPullReplicationsUpdated { get; set; }

            public bool RavenEtlsUpdated { get; set; }

            public bool SqlEtlsUpdated { get; set; }

            public bool ExternalReplicationsUpdated { get; set; }

            public bool PeriodicBackupsUpdated { get; set; }

            public bool ConflictSolverConfigUpdated { get; set; }

            public bool RevisionsConfigurationUpdated { get; set; }

            public bool ExpirationConfigurationUpdated { get; set; }

            public bool RefreshConfigurationUpdated { get; set; }

            public bool RavenConnectionStringsUpdated { get; set; }

            public bool SqlConnectionStringsUpdated { get; set; }

            public bool ClientConfigurationUpdated { get; set; }

            public bool UnusedDatabaseIdsUpdated { get; set; }

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();

                if (RevisionsConfigurationUpdated)
                    json[nameof(RevisionsConfigurationUpdated)] = RevisionsConfigurationUpdated;

                if (ExpirationConfigurationUpdated)
                    json[nameof(ExpirationConfigurationUpdated)] = ExpirationConfigurationUpdated;

                if (RefreshConfigurationUpdated)
                    json[nameof(RefreshConfigurationUpdated)] = RefreshConfigurationUpdated;

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

                if (SinkPullReplicationsUpdated)
                    json[nameof(SinkPullReplicationsUpdated)] = SinkPullReplicationsUpdated;

                if (HubPullReplicationsUpdated)
                    json[nameof(HubPullReplicationsUpdated)] = HubPullReplicationsUpdated;

                if (UnusedDatabaseIdsUpdated)
                    json[nameof(UnusedDatabaseIdsUpdated)] = UnusedDatabaseIdsUpdated;

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

                if (SinkPullReplicationsUpdated)
                    sb.AppendLine("- Pull Replication Sinks");

                if (HubPullReplicationsUpdated)
                    sb.AppendLine("- Pull Replication Hubs");

                if (ClientConfigurationUpdated)
                    sb.AppendLine("- Client");

                if (UnusedDatabaseIdsUpdated)
                    sb.AppendLine("- Unused Database IDs");

                if (sb.Length == 0)
                    return string.Empty;

                sb.Insert(0, "Following configurations were updated:" + Environment.NewLine);

                return sb.ToString();
            }
        }

        public class Counts
        {
            public bool Skipped { get; set; }

            public bool Processed { get; set; }

            public long ReadCount { get; set; }

            public long ErroredCount { get; set; }

            public virtual DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue
                {
                    [nameof(Processed)] = Processed,
                    [nameof(Skipped)] = Skipped,
                    [nameof(ReadCount)] = ReadCount,
                    [nameof(ErroredCount)] = ErroredCount
                };
            }

            public override string ToString()
            {
                return $"Read: {ReadCount:#,#;;0}. " +
                       $"Errored: {ErroredCount:#,#;;0}.";
            }
        }

        public class CountsWithLastEtag : Counts
        {
            public long LastEtag { get; set; }

            public Counts Attachments { get; set; } = new Counts();

            public override DynamicJsonValue ToJson()
            {
                var json = base.ToJson();
                json[nameof(LastEtag)] = LastEtag;
                json[nameof(Attachments)] = Attachments.ToJson();
                return json;
            }

            public override string ToString()
            {
                return $"{base.ToString()} Attachments: {Attachments}";
            }
        }

        public class CountsWithSkippedCountAndLastEtag : CountsWithLastEtag
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
    }
}
