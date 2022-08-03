using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.OngoingTasks;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal abstract class ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    protected ShardedOngoingTasksHandlerProcessorForGetOngoingTasksInfo([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(databaseRecord.DatabaseName)))
        {
            var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);

            yield return new OngoingTaskSubscription
            {
                TaskName = subscriptionState.SubscriptionName,
                TaskState = subscriptionState.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                TaskId = subscriptionState.SubscriptionId,
                Query = subscriptionState.Query,
            };
        }
    }

    protected override IEnumerable<OngoingTaskBackup> CollectBackupTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.PeriodicBackups != null)
        {
            foreach (var backup in databaseRecord.PeriodicBackups)
            {
                yield return new OngoingTaskBackup()
                {
                    TaskId = backup.TaskId,
                    TaskName = backup.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskRavenEtlListView> CollectRavenEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.RavenEtls != null)
        {
            foreach (var etl in databaseRecord.RavenEtls)
            {
                yield return new OngoingTaskRavenEtlListView()
                {
                    TaskId = etl.TaskId,
                    TaskName = etl.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskSqlEtlListView> CollectSqlEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.SqlEtls != null)
        {
            foreach (var etl in databaseRecord.SqlEtls)
            {
                yield return new OngoingTaskSqlEtlListView()
                {
                    TaskId = etl.TaskId,
                    TaskName = etl.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskOlapEtlListView> CollectOlapEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.OlapEtls != null)
        {
            foreach (var etl in databaseRecord.OlapEtls)
            {
                yield return new OngoingTaskOlapEtlListView()
                {
                    TaskId = etl.TaskId,
                    TaskName = etl.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskElasticSearchEtlListView> CollectElasticEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.ElasticSearchEtls != null)
        {
            foreach (var etl in databaseRecord.ElasticSearchEtls)
            {
                yield return new OngoingTaskElasticSearchEtlListView()
                {
                    TaskId = etl.TaskId,
                    TaskName = etl.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskQueueEtlListView> CollectQueueEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.QueueEtls != null)
        {
            foreach (var etl in databaseRecord.QueueEtls)
            {
                yield return new OngoingTaskQueueEtlListView()
                {
                    TaskId = etl.TaskId,
                    TaskName = etl.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.ExternalReplications != null)
        {
            foreach (var replication in databaseRecord.ExternalReplications)
            {
                yield return new OngoingTaskReplication()
                {
                    TaskId = replication.TaskId,
                    TaskName = replication.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.SinkPullReplications != null)
        {
            foreach (var replicationAsSink in databaseRecord.SinkPullReplications)
            {
                yield return new OngoingTaskPullReplicationAsSink()
                {
                    TaskId = replicationAsSink.TaskId,
                    TaskName = replicationAsSink.Name,
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsHub> CollectPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.HubPullReplications != null)
        {
            foreach (var replicationDefinition in databaseRecord.HubPullReplications)
            {
                yield return new OngoingTaskPullReplicationAsHub()
                {
                    TaskId = replicationDefinition.TaskId,
                    TaskName = replicationDefinition.Name,
                };
            }
        }
    }

    protected override int SubscriptionsCount
    {
        get
        {
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "RavenDB-19069 Fix SubscriptionsCount");
            return -1;
        }
    }

    protected override ValueTask<(string Url, OngoingTaskConnectionStatus Status)> GetReplicationTaskConnectionStatusAsync<T>(DatabaseTopology databaseTopology,
        ClusterTopology clusterTopology, T replication, Dictionary<string, RavenConnectionString> connectionStrings,
        out string tag, out RavenConnectionString connection)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13110");

        (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.None);

        if (replication is PullReplicationAsSink)
        {
            res.Status = OngoingTaskConnectionStatus.NotActive;
        }

        tag = null;
        connection = null;

        return ValueTask.FromResult(res);
    }

    protected override ValueTask<PeriodicBackupStatus> GetBackupStatusAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag,
        out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13113");

        responsibleNodeTag = null;
        nextBackup = null;
        onGoingBackup = null;
        isEncrypted = false;

        PeriodicBackupStatus res = null;
        return ValueTask.FromResult(res);
    }

    protected override ValueTask<OngoingTaskConnectionStatus> GetSubscriptionConnectionStatusAsync(DatabaseRecord record, SubscriptionState subscriptionState, long key,
        out string tag)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13113");

        tag = null;
        OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
        return ValueTask.FromResult(connectionStatus);
    }

    protected override ValueTask<OngoingTaskConnectionStatus> GetEtlTaskConnectionStatusAsync<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 implement for sharding");

        var connectionStatus = OngoingTaskConnectionStatus.None;
        error = null;
        tag = null;

        return ValueTask.FromResult(connectionStatus);
    }

    protected override ValueTask<RavenEtl> GetProcess(RavenEtlConfiguration config)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "RavenDB-19069 implement for sharding");

        RavenEtl process = null;
        return ValueTask.FromResult(process);
    }
}
