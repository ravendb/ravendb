using System;
using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents.OngoingTasks;
using Raven.Server.Documents.Sharding.Subscriptions;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Sharding;

public partial class ShardedDatabaseContext
{
    public ShardedOngoingTasks OngoingTasks;

    public class ShardedOngoingTasks : AbstractOngoingTasks<SubscriptionConnectionsStateOrchestrator>
    {
        private readonly ShardedDatabaseContext _context;

        public ShardedOngoingTasks([NotNull] ShardedDatabaseContext context) : base(context.ServerStore, context.SubscriptionsStorage)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        protected override DatabaseTopology GetDatabaseTopology(DatabaseRecord databaseRecord)
        {
            return databaseRecord.Sharding.Orchestrator.Topology;
        }

        protected override string GetDestinationUrlForRavenEtl(string name) => null;

        protected override IEnumerable<OngoingTaskPullReplicationAsHub> GetPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
        {
            throw new System.NotImplementedException();
        }

        protected override OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
        {
            tag = null;
            error = null;
            return OngoingTaskConnectionStatus.None;
        }

        protected override (string Url, OngoingTaskConnectionStatus Status) GetReplicationTaskConnectionStatus<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, T replication,
            Dictionary<string, RavenConnectionString> connectionStrings, out string tag, out RavenConnectionString connection)
        {
            throw new System.NotImplementedException();
        }

        protected override PeriodicBackupStatus GetBackupStatus(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag,
            out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted)
        {
            nextBackup = null;
            onGoingBackup = null;
            isEncrypted = false;
            responsibleNodeTag = null;
            return null;
        }
    }
}
