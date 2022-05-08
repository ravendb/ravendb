using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;


internal class OngoingTasksHandlerProcessorForGetOngoingTaskInfo : OngoingTasksHandlerProcessorForGetOngoingTasksInfo
{
    public OngoingTasksHandlerProcessorForGetOngoingTaskInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        await GetOngoingTaskInfoInternal();
    }
}

internal class OngoingTasksHandlerProcessorForGetOngoingTasks : OngoingTasksHandlerProcessorForGetOngoingTasksInfo
{
    public OngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        var result = GetOngoingTasksInternal();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, result.ToJson());
        }
    }
}

internal abstract class OngoingTasksHandlerProcessorForGetOngoingTasksInfo : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<DatabaseRequestHandler, DocumentsOperationContext>
{
    [NotNull]
    private readonly DocumentDatabase _database;
    private readonly ServerStore _server;

    protected OngoingTasksHandlerProcessorForGetOngoingTasksInfo([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
        _database = requestHandler.Database;
        _server = requestHandler.ServerStore;
    }

    protected override IEnumerable<OngoingTaskBackup> CollectBackupTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        if (databaseRecord.PeriodicBackups == null)
            yield break;

        if (databaseRecord.PeriodicBackups.Count == 0)
            yield break;

        foreach (var backupConfiguration in databaseRecord.PeriodicBackups)
        {
            yield return GetOngoingTaskBackup(backupConfiguration.TaskId, databaseRecord, backupConfiguration, clusterTopology);
        }
    }

    protected override IEnumerable<OngoingTaskRavenEtlListView> CollectRavenEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.RavenEtls != null)
        {
            foreach (var ravenEtl in databaseRecord.RavenEtls)
            {
                var taskState = OngoingTasksHandler.GetEtlTaskState(ravenEtl);

                databaseRecord.RavenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var connection);

                var process = _database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == ravenEtl.Name);

                var connectionStatus = GetEtlTaskConnectionStatusAsync(databaseRecord, ravenEtl, out var tag, out var error).Result;

                yield return new OngoingTaskRavenEtlListView()
                {
                    TaskId = ravenEtl.TaskId,
                    TaskName = ravenEtl.Name,
                    TaskState = taskState,
                    MentorNode = ravenEtl.MentorNode,
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    DestinationUrl = process?.Url,
                    TaskConnectionStatus = connectionStatus,
                    DestinationDatabase = connection?.Database,
                    ConnectionStringName = ravenEtl.ConnectionStringName,
                    TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
                    Error = error
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskSqlEtlListView> CollectSqlEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.SqlEtls != null)
        {
            foreach (var sqlEtl in databaseRecord.SqlEtls)
            {
                string database = null;
                string server = null;

                if (databaseRecord.SqlConnectionStrings.TryGetValue(sqlEtl.ConnectionStringName, out var sqlConnection))
                {
                    (database, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(sqlConnection.FactoryName, sqlConnection.ConnectionString);
                }

                var connectionStatus = GetEtlTaskConnectionStatusAsync(databaseRecord, sqlEtl, out var tag, out var error).Result;

                var taskState = OngoingTasksHandler.GetEtlTaskState(sqlEtl);

                yield return new OngoingTaskSqlEtlListView
                {
                    TaskId = sqlEtl.TaskId,
                    TaskName = sqlEtl.Name,
                    TaskConnectionStatus = connectionStatus,
                    TaskState = taskState,
                    MentorNode = sqlEtl.MentorNode,
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    DestinationServer = server,
                    DestinationDatabase = database,
                    ConnectionStringDefined = sqlConnection != null,
                    ConnectionStringName = sqlEtl.ConnectionStringName,
                    Error = error
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskOlapEtlListView> CollectOlapEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.OlapEtls != null)
        {
            foreach (var olapEtl in databaseRecord.OlapEtls)
            {
                string destination = default;
                if (databaseRecord.OlapConnectionStrings.TryGetValue(olapEtl.ConnectionStringName, out var olapConnection))
                {
                    destination = olapConnection.GetDestination();
                }

                var connectionStatus = GetEtlTaskConnectionStatusAsync(databaseRecord, olapEtl, out var tag, out var error).Result;

                var taskState = OngoingTasksHandler.GetEtlTaskState(olapEtl);

                yield return new OngoingTaskOlapEtlListView
                {
                    TaskId = olapEtl.TaskId,
                    TaskName = olapEtl.Name,
                    TaskConnectionStatus = connectionStatus,
                    TaskState = taskState,
                    MentorNode = olapEtl.MentorNode,
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    ConnectionStringName = olapEtl.ConnectionStringName,
                    Destination = destination,
                    Error = error
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskElasticSearchEtlListView> CollectElasticEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.ElasticSearchEtls != null)
        {
            foreach (var elasticSearchEtl in databaseRecord.ElasticSearchEtls)
            {
                databaseRecord.ElasticSearchConnectionStrings.TryGetValue(elasticSearchEtl.ConnectionStringName, out var connection);

                var connectionStatus = GetEtlTaskConnectionStatusAsync(databaseRecord, elasticSearchEtl, out var tag, out var error).Result;
                var taskState = OngoingTasksHandler.GetEtlTaskState(elasticSearchEtl);

                yield return new OngoingTaskElasticSearchEtlListView
                {
                    TaskId = elasticSearchEtl.TaskId,
                    TaskName = elasticSearchEtl.Name,
                    TaskConnectionStatus = connectionStatus,
                    TaskState = taskState,
                    MentorNode = elasticSearchEtl.MentorNode,
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    ConnectionStringName = elasticSearchEtl.ConnectionStringName,
                    NodesUrls = connection?.Nodes,
                    Error = error
                };
            }
        }
    }

    protected override IEnumerable<OngoingTask> CollectEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            return Enumerable.Empty<OngoingTask>();

        return base.CollectEtlTasks(context, clusterTopology, databaseRecord);
    }

    protected override ValueTask<OngoingTaskConnectionStatus> GetEtlTaskConnectionStatusAsync<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
    {
        var connectionStatus = OngoingTaskConnectionStatus.None;
        error = null;

        var processState = EtlLoader.GetProcessState(config.Transforms, _database, config.Name);

        tag = _database.WhoseTaskIsIt(record.Topology, config, processState);

        if (tag == _server.NodeTag)
        {
            var process = _database.EtlLoader.Processes.FirstOrDefault(x => x.ConfigurationName == config.Name);

            if (process != null)
                connectionStatus = process.GetConnectionStatus();
            else
            {
                if (config.Disabled)
                    connectionStatus = OngoingTaskConnectionStatus.NotActive;
                else
                    error = $"ETL process '{config.Name}' was not found.";
            }
        }
        else
        {
            connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        return ValueTask.FromResult(connectionStatus);
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        foreach (var edgeReplication in databaseRecord.SinkPullReplications)
        {
            yield return GetPullReplicationAsSinkInfo(databaseRecord.Topology, clusterTopology, databaseRecord, edgeReplication);
        }
    }

    protected override IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        foreach (var externalReplication in databaseRecord.ExternalReplications)
        {
            yield return GetExternalReplicationInfo(databaseRecord.Topology, clusterTopology, externalReplication, databaseRecord.RavenConnectionStrings);
        }
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsHub> CollectPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        var pullReplicationHandlers = _database.ReplicationLoader.OutgoingHandlers.Where(n => n is OutgoingPullReplicationHandler).ToList();
        foreach (var handler in pullReplicationHandlers)
        {
            var ex = handler.Destination as ExternalReplication;
            if (ex == null) // should not happened
                continue;

            yield return GetPullReplicationAsHubTaskInfo(clusterTopology, ex);
        }
    }

    protected override IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var keyValue in ClusterStateMachine.ReadValuesStartingWith(context, SubscriptionState.SubscriptionPrefix(databaseRecord.DatabaseName)))
        {
            var subscriptionState = JsonDeserializationClient.SubscriptionState(keyValue.Value);
            var tag = _database.WhoseTaskIsIt(databaseRecord.Topology, subscriptionState, subscriptionState);
            OngoingTaskConnectionStatus connectionStatus;
            if (tag != _server.NodeTag)
            {
                connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
            }
            else if (_database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(subscriptionState.SubscriptionId, out var connectionsState))
            {
                connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
            }
            else
            {
                connectionStatus = OngoingTaskConnectionStatus.NotActive;
            }

            yield return new OngoingTaskSubscription
            {
                // Supply only needed fields for List View
                ResponsibleNode = new NodeId
                {
                    NodeTag = tag,
                    NodeUrl = clusterTopology.GetUrlFromTag(tag)
                },
                TaskName = subscriptionState.SubscriptionName,
                TaskState = subscriptionState.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                MentorNode = subscriptionState.MentorNode,
                TaskId = subscriptionState.SubscriptionId,
                Query = subscriptionState.Query,
                TaskConnectionStatus = connectionStatus
            };
        }
    }

    protected override ValueTask<(string Url, OngoingTaskConnectionStatus Status)> GetReplicationTaskConnectionStatusAsync<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
        T replication, Dictionary<string, RavenConnectionString> connectionStrings, out string tag, out RavenConnectionString connection)
    {
        connectionStrings.TryGetValue(replication.ConnectionStringName, out connection);
        replication.Database = connection?.Database;
        replication.ConnectionString = connection;

        var taskStatus = ReplicationLoader.GetExternalReplicationState(_server, RequestHandler.DatabaseName, replication.TaskId);
        tag = _database.WhoseTaskIsIt(databaseTopology, replication, taskStatus);

        (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.None);

        if (replication is ExternalReplication)
        {
            if (tag == _server.NodeTag)
                res = _database.ReplicationLoader.GetExternalReplicationDestination(replication.TaskId);
            else
                res.Status = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        else if (replication is PullReplicationAsSink sinkReplication)
        {
            res.Status = OngoingTaskConnectionStatus.NotActive;
            var handlers = GetIncomingHandlers();
            if (tag == ServerStore.NodeTag)
            {
                foreach (var incoming in handlers)
                {
                    if (incoming is IncomingPullReplicationHandler pullHandler &&
                        pullHandler._incomingPullReplicationParams?.Name == sinkReplication.HubName)
                    {
                        res = (incoming.ConnectionInfo.SourceUrl, OngoingTaskConnectionStatus.Active);
                        break;
                    }
                }
            }
            else
            {
                res.Status = OngoingTaskConnectionStatus.NotOnThisNode;
            }
        }

        return ValueTask.FromResult(res);
    }

    protected override ValueTask<RavenEtl> GetProcess(RavenEtlConfiguration config)
    {
        var process = RequestHandler.Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == config.Name);
        return ValueTask.FromResult(process);
    }

    protected override ValueTask<OngoingTaskConnectionStatus> GetSubscriptionConnectionStatusAsync(DatabaseRecord record, SubscriptionState subscriptionState, long key,
        out string tag)
    {
        tag = RequestHandler.Database.WhoseTaskIsIt(record.Topology, subscriptionState, subscriptionState);
        OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
        if (tag != ServerStore.NodeTag)
        {
            connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
        }
        else if (RequestHandler.Database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(key, out var connectionsState))
        {
            connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
        }

        return ValueTask.FromResult(connectionStatus);
    }

    protected List<IncomingReplicationHandler> GetIncomingHandlers() => RequestHandler.Database.ReplicationLoader.IncomingHandlers.ToList();

    protected override int SubscriptionsCount => (int)_database.SubscriptionStorage.GetAllSubscriptionsCount();

    private OngoingTaskPullReplicationAsHub GetPullReplicationAsHubTaskInfo(ClusterTopology clusterTopology, ExternalReplication ex)
    {
        var connectionResult = _database.ReplicationLoader.GetPullReplicationDestination(ex.TaskId, ex.Database);
        var tag = _server.NodeTag; // we can't know about pull replication tasks on other nodes.

        return new OngoingTaskPullReplicationAsHub
        {
            TaskId = ex.TaskId,
            TaskName = ex.Name,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            TaskState = ex.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
            DestinationDatabase = ex.Database,
            DestinationUrl = connectionResult.Url,
            MentorNode = ex.MentorNode,
            TaskConnectionStatus = connectionResult.Status,
            DelayReplicationFor = ex.DelayReplicationFor
        };
    }

    protected override ValueTask<PeriodicBackupStatus> GetBackupStatusAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration,
        out string responsibleNodeTag, out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted)
    {
        var backupStatus = RequestHandler.Database.PeriodicBackupRunner.GetBackupStatus(taskId);
        responsibleNodeTag = RequestHandler.Database.WhoseTaskIsIt(databaseRecord.Topology, backupConfiguration, backupStatus, keepTaskOnOriginalMemberNode: true);
        nextBackup = RequestHandler.Database.PeriodicBackupRunner.GetNextBackupDetails(databaseRecord, backupConfiguration, backupStatus, responsibleNodeTag);
        onGoingBackup = RequestHandler.Database.PeriodicBackupRunner.OnGoingBackup(taskId);
        isEncrypted = BackupTask.IsBackupEncrypted(RequestHandler.Database, backupConfiguration);

        return ValueTask.FromResult(backupStatus);
    }
}
