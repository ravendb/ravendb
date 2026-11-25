using System;
using System.Runtime.CompilerServices;
using Raven.Server.ServerWide.Commands;
using Raven.Server.ServerWide.Commands.CdcSink;
using Raven.Server.ServerWide.Commands.ETL;
using Raven.Server.ServerWide.Commands.Monitoring.Snmp;
using Raven.Server.ServerWide.Commands.PeriodicBackup;
using Raven.Server.ServerWide.Commands.QueueSink;
using Raven.Server.ServerWide.Commands.Sharding;
using Raven.Server.ServerWide.Commands.Subscriptions;

namespace Raven.Server.ServerWide
{
    /// <summary>
    /// The database features that need to be notified about a value change (a cluster value of the database, not its record).
    /// </summary>
    [Flags]
    public enum DatabaseValueChangeConsumers
    {
        None = 0,
        Subscriptions = 1,
        Etl = 2,

        All = Subscriptions | Etl
    }

    public static class DatabaseValueChangeConsumersExtensions
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static bool Contain(this DatabaseValueChangeConsumers current, DatabaseValueChangeConsumers flag)
        {
            return (current & flag) == flag;
        }

        public static DatabaseValueChangeConsumers ForType(string type)
        {
            switch (type)
            {
                case nameof(PutSubscriptionCommand):
                case nameof(PutShardedSubscriptionCommand):
                case nameof(PutSubscriptionBatchCommand):
                case nameof(PutShardedSubscriptionBatchCommand):
                case nameof(DeleteSubscriptionCommand):
                case nameof(ToggleSubscriptionStateCommand):
                case nameof(AcknowledgeSubscriptionBatchCommand):
                case nameof(RecordBatchSubscriptionDocumentsCommand):
                case nameof(UpdateSubscriptionClientConnectionTime):
                    return DatabaseValueChangeConsumers.Subscriptions;

                case nameof(UpdateEtlProcessStateCommand):
                case nameof(RemoveEtlProcessStateCommand):
                    return DatabaseValueChangeConsumers.Etl;

                // handled by the periodic backup runner on its own
                case nameof(UpdatePeriodicBackupStatusCommand):
                case nameof(DelayBackupCommand):
                // not relevant for the database features
                case nameof(IncrementClusterIdentityCommand):
                case nameof(IncrementClusterIdentitiesBatchCommand):
                case nameof(UpdateClusterIdentityCommand):
                case nameof(CleanCompareExchangeTombstonesCommand):
                case nameof(UpdateExternalReplicationStateCommand):
                case nameof(ShardedUpdateExternalReplicationStateCommand):
                case nameof(RegisterReplicationHubAccessCommand):
                case nameof(UnregisterReplicationHubAccessCommand):
                case nameof(BulkRegisterReplicationHubAccessCommand):
                case nameof(UpdateQueueSinkProcessStateCommand):
                case nameof(RemoveQueueSinkProcessStateCommand):
                case nameof(UpdateCdcSinkProcessStateCommand):
                case nameof(RemoveCdcSinkProcessStateCommand):
                case nameof(UpdateSnmpDatabaseIndexesMappingCommand):
                case nameof(UpdateSnmpDatabaseEtlsMappingCommand):
                case nameof(UpdateSnmpDatabaseAiTasksMappingCommand):
                case nameof(UpdateSnmpDatabaseCdcSinksMappingCommand):
                    return DatabaseValueChangeConsumers.None;

                // e.g. UpdateResponsibleNodeForTasksCommand
                default:
                    return DatabaseValueChangeConsumers.All;
            }
        }
    }
}
