using System.Collections.Generic;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System.Processors.OngoingTasks;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTasks : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<TransactionOperationContext>
{
    [NotNull]
    private readonly ShardedDatabaseRequestHandler _handler;

    public ShardedOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
    {
        _handler = requestHandler;
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
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Fix SubscriptionsCount");
            return -1;
        }
    }
}
