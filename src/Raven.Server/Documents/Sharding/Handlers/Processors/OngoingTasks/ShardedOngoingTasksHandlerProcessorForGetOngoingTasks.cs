using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.Http;
using Raven.Server.Web.System.Processors.OngoingTasks;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.OngoingTasks;

internal class ShardedOngoingTasksHandlerProcessorForGetOngoingTasks : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<ShardedDatabaseRequestHandler, TransactionOperationContext>
{
    public ShardedOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
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

    private ProxyCommand<OngoingTask> CreateProxyCommandForShard()
    {
        var (key, taskName, type) = TryGetParameters();

        var command = taskName == null ? 
            new GetOngoingTaskInfoOperation.GetOngoingTaskInfoCommand(key, type) : 
            new GetOngoingTaskInfoOperation.GetOngoingTaskInfoCommand(taskName, type);

        return new ProxyCommand<OngoingTask>(command, RequestHandler.HttpContext.Response);
    }

    protected override async ValueTask<OngoingTaskSubscription> GetSubscriptionTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology,
        SubscriptionState subscriptionState, long key)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13113");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskSubscription)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskReplication> GetExternalReplicationInfoAsync(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
        ExternalReplication watcher, Dictionary<string, RavenConnectionString> connectionStrings)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13110");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskReplication)proxyCommand.Result;
    }

    protected override List<IncomingReplicationHandler> GetIncomingHandlers() => null;

    protected override async ValueTask<OngoingTaskPullReplicationAsSink> GetPullReplicationAsSinkInfoAsync(DatabaseTopology dbTopology, ClusterTopology clusterTopology,
        Dictionary<string, RavenConnectionString> connectionStrings, PullReplicationAsSink sinkReplication, List<IncomingReplicationHandler> handlers)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13110");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskPullReplicationAsSink)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskBackup> GetOngoingTaskBackupAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, ClusterTopology clusterTopology)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding - https://issues.hibernatingrhinos.com/issue/RavenDB-13112");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskBackup)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskSqlEtlDetails> GetSqlEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, SqlEtlConfiguration config)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskSqlEtlDetails)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskOlapEtlDetails> GetOlapEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, OlapEtlConfiguration config)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskOlapEtlDetails)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskRavenEtlDetails> GetRavenEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, RavenEtlConfiguration config)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskRavenEtlDetails)proxyCommand.Result;
    }

    protected override async ValueTask<OngoingTaskElasticSearchEtlDetails> GetElasticSearchEtTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, ElasticSearchEtlConfiguration config)
    {
        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Shiran, DevelopmentHelper.Severity.Normal, "implement for sharding");

        var proxyCommand = CreateProxyCommandForShard();
        await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxyCommand, 0);

        return (OngoingTaskElasticSearchEtlDetails)proxyCommand.Result;
    }
}
