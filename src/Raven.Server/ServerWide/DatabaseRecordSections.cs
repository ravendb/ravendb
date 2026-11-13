using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.ServerWide;

namespace Raven.Server.ServerWide
{
    [Flags]
    public enum DatabaseRecordSections
    {
        None = 0,

        Topology = 1 << 0,
        Indexes = 1 << 1,
        Sorters = 1 << 2,
        Analyzers = 1 << 3,
        Replication = 1 << 4,
        ConnectionStrings = 1 << 5,
        Etls = 1 << 6,
        EmbeddingsGenerations = 1 << 7,
        PeriodicBackups = 1 << 8,
        QueueSinks = 1 << 9,
        CdcSinks = 1 << 10,

        // the rest of the record, it isn't tracked and is handled on every record change
        Configuration = 1 << 11,

        All = Topology | Indexes | Sorters | Analyzers | Replication | ConnectionStrings | Etls | EmbeddingsGenerations |
              PeriodicBackups | QueueSinks | CdcSinks | Configuration
    }

    public static class DatabaseRecordSectionsExtensions
    {
        // every field of the database record must be mapped here (verified by a test)
        internal static readonly Dictionary<string, DatabaseRecordSections> SectionByField = new()
        {
            [nameof(DatabaseRecord.Topology)] = DatabaseRecordSections.Topology,
            [nameof(DatabaseRecord.DeletionInProgress)] = DatabaseRecordSections.Topology,
            [nameof(DatabaseRecord.Sharding)] = DatabaseRecordSections.Topology,

            [nameof(DatabaseRecord.Indexes)] = DatabaseRecordSections.Indexes,
            [nameof(DatabaseRecord.AutoIndexes)] = DatabaseRecordSections.Indexes,
            [nameof(DatabaseRecord.IndexesHistory)] = DatabaseRecordSections.Indexes,
            [nameof(DatabaseRecord.RollingIndexes)] = DatabaseRecordSections.Indexes,

            [nameof(DatabaseRecord.Sorters)] = DatabaseRecordSections.Sorters,
            [nameof(DatabaseRecord.Analyzers)] = DatabaseRecordSections.Analyzers,

            [nameof(DatabaseRecord.ExternalReplications)] = DatabaseRecordSections.Replication,
            [nameof(DatabaseRecord.HubPullReplications)] = DatabaseRecordSections.Replication,
            [nameof(DatabaseRecord.SinkPullReplications)] = DatabaseRecordSections.Replication,
            [nameof(DatabaseRecord.ConflictSolverConfig)] = DatabaseRecordSections.Replication,
            [nameof(DatabaseRecord.SupportedFeatures)] = DatabaseRecordSections.Replication,

            [nameof(DatabaseRecord.RavenConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.SqlConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.OlapConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.ElasticSearchConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.QueueConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.SnowflakeConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,
            [nameof(DatabaseRecord.AiConnectionStrings)] = DatabaseRecordSections.ConnectionStrings,

            [nameof(DatabaseRecord.RavenEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.SqlEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.OlapEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.ElasticSearchEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.QueueEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.SnowflakeEtls)] = DatabaseRecordSections.Etls,
            [nameof(DatabaseRecord.GenAis)] = DatabaseRecordSections.Etls,

            [nameof(DatabaseRecord.EmbeddingsGenerations)] = DatabaseRecordSections.EmbeddingsGenerations,
            [nameof(DatabaseRecord.PeriodicBackups)] = DatabaseRecordSections.PeriodicBackups,
            [nameof(DatabaseRecord.QueueSinks)] = DatabaseRecordSections.QueueSinks,
            [nameof(DatabaseRecord.CdcSinks)] = DatabaseRecordSections.CdcSinks,

            [nameof(DatabaseRecord.DatabaseName)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Disabled)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Encrypted)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.DatabaseState)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.LockMode)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.SchemaValidation)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.DocumentsCompression)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Settings)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Revisions)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.RevisionsBin)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.RevisionsForConflicts)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.TimeSeries)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Expiration)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Refresh)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.DataArchival)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.RemoteAttachments)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Integrations)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.AiAgents)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Client)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.Studio)] = DatabaseRecordSections.Configuration,
            [nameof(DatabaseRecord.UnusedDatabaseIds)] = DatabaseRecordSections.Configuration,

            // not handled by the database on record change
            [nameof(DatabaseRecord.EtagForBackup)] = DatabaseRecordSections.None,
            [nameof(DatabaseRecord.TruncatedClusterTransactionCommandsCount)] = DatabaseRecordSections.None,
            [nameof(DatabaseRecord.ClusterState)] = DatabaseRecordSections.None,
        };

        // the sections that the database handles on every record change, regardless of the sections that were changed
        internal const DatabaseRecordSections HandledOnEveryChange = DatabaseRecordSections.Configuration | DatabaseRecordSections.Topology;

        /// <summary>
        /// Whether the field is needed to handle a record change that none of the features (indexes, ETLs, backups etc.) has to handle,
        /// see <see cref="JsonDeserializationCluster.DatabaseRecordForEveryChange"/>.
        /// </summary>
        internal static bool IsHandledOnEveryChange(string field)
        {
            // part of the replication section, but it is small and is read on every change
            if (field == nameof(DatabaseRecord.SupportedFeatures))
                return true;

            if (SectionByField.TryGetValue(field, out var section) == false)
                return true;

            return HandledOnEveryChange.Contain(section);
        }

        public static DatabaseRecordSections ForTaskType(OngoingTaskType taskType)
        {
            switch (taskType)
            {
                case OngoingTaskType.Replication:
                case OngoingTaskType.PullReplicationAsHub:
                case OngoingTaskType.PullReplicationAsSink:
                    return DatabaseRecordSections.Replication;

                case OngoingTaskType.RavenEtl:
                case OngoingTaskType.SqlEtl:
                case OngoingTaskType.OlapEtl:
                case OngoingTaskType.ElasticSearchEtl:
                case OngoingTaskType.QueueEtl:
                case OngoingTaskType.SnowflakeEtl:
                case OngoingTaskType.GenAi:
                    return DatabaseRecordSections.Etls;

                case OngoingTaskType.EmbeddingsGeneration:
                    return DatabaseRecordSections.EmbeddingsGenerations;

                case OngoingTaskType.Backup:
                    return DatabaseRecordSections.PeriodicBackups;

                case OngoingTaskType.QueueSink:
                    return DatabaseRecordSections.QueueSinks;

                case OngoingTaskType.CdcSink:
                    return DatabaseRecordSections.CdcSinks;

                default:
                    return DatabaseRecordSections.All;
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this DatabaseRecordSections current, DatabaseRecordSections flag)
        {
            return (current & flag) == flag;
        }

        public static void Stamp(this DatabaseRecordClusterState state, DatabaseRecordSections sections, long index)
        {
            if (sections.Contain(DatabaseRecordSections.Topology))
                state.LastTopologyIndex = index;
            if (sections.Contain(DatabaseRecordSections.Indexes))
                state.LastIndexesIndex = index;
            if (sections.Contain(DatabaseRecordSections.Sorters))
                state.LastSortersIndex = index;
            if (sections.Contain(DatabaseRecordSections.Analyzers))
                state.LastAnalyzersIndex = index;
            if (sections.Contain(DatabaseRecordSections.Replication))
                state.LastReplicationIndex = index;
            if (sections.Contain(DatabaseRecordSections.ConnectionStrings))
                state.LastConnectionStringsIndex = index;
            if (sections.Contain(DatabaseRecordSections.Etls))
                state.LastEtlsIndex = index;
            if (sections.Contain(DatabaseRecordSections.EmbeddingsGenerations))
                state.LastEmbeddingsGenerationsIndex = index;
            if (sections.Contain(DatabaseRecordSections.PeriodicBackups))
                state.LastPeriodicBackupsIndex = index;
            if (sections.Contain(DatabaseRecordSections.QueueSinks))
                state.LastQueueSinksIndex = index;
            if (sections.Contain(DatabaseRecordSections.CdcSinks))
                state.LastCdcSinksIndex = index;
        }

        /// <summary>
        /// Returns the raft index of the last change to any of the given sections, 0 if it is unknown.
        /// </summary>
        public static long GetVersion(this DatabaseRecordClusterState state, DatabaseRecordSections sections)
        {
            if (state == null)
                return 0;

            long version = 0;

            if (sections.Contain(DatabaseRecordSections.Topology))
                version = Math.Max(version, state.LastTopologyIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.Indexes))
                version = Math.Max(version, state.LastIndexesIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.Sorters))
                version = Math.Max(version, state.LastSortersIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.Analyzers))
                version = Math.Max(version, state.LastAnalyzersIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.Replication))
                version = Math.Max(version, state.LastReplicationIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.ConnectionStrings))
                version = Math.Max(version, state.LastConnectionStringsIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.Etls))
                version = Math.Max(version, state.LastEtlsIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.EmbeddingsGenerations))
                version = Math.Max(version, state.LastEmbeddingsGenerationsIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.PeriodicBackups))
                version = Math.Max(version, state.LastPeriodicBackupsIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.QueueSinks))
                version = Math.Max(version, state.LastQueueSinksIndex ?? 0);
            if (sections.Contain(DatabaseRecordSections.CdcSinks))
                version = Math.Max(version, state.LastCdcSinksIndex ?? 0);

            return version;
        }
    }
}
