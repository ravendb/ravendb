using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
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

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal class OngoingTasksHandlerProcessorForGetOngoingTasks : AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<DatabaseRequestHandler, DocumentsOperationContext>
{
    [NotNull]
    private readonly DocumentDatabase _database;
    private readonly ServerStore _server;

    public OngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
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
            yield return GetOngoingTaskBackupAsync(backupConfiguration.TaskId, databaseRecord, backupConfiguration, clusterTopology).Result;
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

                var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, ravenEtl, out var tag, out var error);

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

                var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, sqlEtl, out var tag, out var error);

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

                var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, olapEtl, out var tag, out var error);

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

                var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, elasticSearchEtl, out var tag, out var error);
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

    private OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
        where T : ConnectionString
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

        return connectionStatus;
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        var handlers = _database.ReplicationLoader.IncomingHandlers.ToList();
        foreach (var edgeReplication in databaseRecord.SinkPullReplications)
        {
            yield return GetPullReplicationAsSinkInfoAsync(databaseRecord.Topology, clusterTopology, databaseRecord.RavenConnectionStrings, edgeReplication, handlers).Result;
        }
    }

    protected override IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        foreach (var externalReplication in databaseRecord.ExternalReplications)
        {
            yield return GetExternalReplicationInfoAsync(databaseRecord.Topology, clusterTopology, externalReplication, databaseRecord.RavenConnectionStrings).Result;
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

    protected override ValueTask<OngoingTaskReplication> GetExternalReplicationInfoAsync(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
        ExternalReplication watcher, Dictionary<string, RavenConnectionString> connectionStrings)
    {
        connectionStrings.TryGetValue(watcher.ConnectionStringName, out var connection);
        watcher.Database = connection?.Database;
        watcher.ConnectionString = connection;

        var taskStatus = ReplicationLoader.GetExternalReplicationState(_server, RequestHandler.DatabaseName, watcher.TaskId);
        var tag = _database.WhoseTaskIsIt(databaseTopology, watcher, taskStatus);

        (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.None);
        if (tag == _server.NodeTag)
        {
            res = _database.ReplicationLoader.GetExternalReplicationDestination(watcher.TaskId);
        }
        else
        {
            res.Status = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        var taskInfo = new OngoingTaskReplication
        {
            TaskId = watcher.TaskId,
            TaskName = watcher.Name,
            ResponsibleNode = new NodeId
            {
                NodeTag = tag,
                NodeUrl = clusterTopology.GetUrlFromTag(tag)
            },
            ConnectionStringName = watcher.ConnectionStringName,
            TaskState = watcher.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
            DestinationDatabase = connection?.Database,
            DestinationUrl = res.Url,
            TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
            MentorNode = watcher.MentorNode,
            TaskConnectionStatus = res.Status,
            DelayReplicationFor = watcher.DelayReplicationFor
        };

        return ValueTask.FromResult(taskInfo);
    }

    protected override ValueTask<OngoingTaskPullReplicationAsSink> GetPullReplicationAsSinkInfoAsync(DatabaseTopology dbTopology, ClusterTopology clusterTopology,
        Dictionary<string, RavenConnectionString> connectionStrings, PullReplicationAsSink sinkReplication, List<IncomingReplicationHandler> handlers)
    {
        connectionStrings.TryGetValue(sinkReplication.ConnectionStringName, out var connection);
        sinkReplication.Database = connection?.Database;
        sinkReplication.ConnectionString = connection;

        var tag = RequestHandler.Database.WhoseTaskIsIt(dbTopology, sinkReplication, null);

        (string Url, OngoingTaskConnectionStatus Status) res = (null, OngoingTaskConnectionStatus.NotActive);
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

        var sinkInfo = new OngoingTaskPullReplicationAsSink
        {
            TaskId = sinkReplication.TaskId,
            TaskName = sinkReplication.Name,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            ConnectionStringName = sinkReplication.ConnectionStringName,
            TaskState = sinkReplication.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
            DestinationDatabase = connection?.Database,
            HubName = sinkReplication.HubName,
            Mode = sinkReplication.Mode,
            DestinationUrl = res.Url,
            TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
            MentorNode = sinkReplication.MentorNode,
            TaskConnectionStatus = res.Status,
            AccessName = sinkReplication.AccessName,
            AllowedHubToSinkPaths = sinkReplication.AllowedHubToSinkPaths,
            AllowedSinkToHubPaths = sinkReplication.AllowedSinkToHubPaths
        };

        if (sinkReplication.CertificateWithPrivateKey != null)
        {
            // fetch public key of certificate
            var certBytes = Convert.FromBase64String(sinkReplication.CertificateWithPrivateKey);
            var certificate = new X509Certificate2(certBytes,
                sinkReplication.CertificatePassword,
                X509KeyStorageFlags.Exportable | X509KeyStorageFlags.MachineKeySet);

            sinkInfo.CertificatePublicKey = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
        }

        return ValueTask.FromResult(sinkInfo);
    }

    protected override ValueTask<OngoingTaskSqlEtlDetails> GetSqlEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, SqlEtlConfiguration config)
    {
        return ValueTask.FromResult(new OngoingTaskSqlEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            MentorNode = config.MentorNode,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            TaskConnectionStatus = GetEtlTaskConnectionStatus(record, config, out var sqlNode, out var sqlEtlError),
            ResponsibleNode = new NodeId
            {
                NodeTag = sqlNode,
                NodeUrl = clusterTopology.GetUrlFromTag(sqlNode)
            },
            Error = sqlEtlError
        });
    }

    protected override ValueTask<OngoingTaskOlapEtlDetails> GetOlapEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, OlapEtlConfiguration config)
    {
        return ValueTask.FromResult(new OngoingTaskOlapEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            MentorNode = config.MentorNode,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            TaskConnectionStatus = GetEtlTaskConnectionStatus(record, config, out var sqlNode, out var sqlEtlError),
            ResponsibleNode = new NodeId
            {
                NodeTag = sqlNode,
                NodeUrl = clusterTopology.GetUrlFromTag(sqlNode)
            },
            Error = sqlEtlError
        });
    }

    protected override ValueTask<OngoingTaskRavenEtlDetails> GetRavenEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, RavenEtlConfiguration config)
    {
        var process = RequestHandler.Database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == config.Name);

        return ValueTask.FromResult(new OngoingTaskRavenEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            MentorNode = config.MentorNode,
            DestinationUrl = process?.Url,
            TaskConnectionStatus = GetEtlTaskConnectionStatus(record, config, out var node, out var ravenEtlError),
            ResponsibleNode = new NodeId
            {
                NodeTag = node,
                NodeUrl = clusterTopology.GetUrlFromTag(node)
            },
            Error = ravenEtlError
        });
    }

    protected override ValueTask<OngoingTaskElasticSearchEtlDetails> GetElasticSearchEtTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, ElasticSearchEtlConfiguration config)
    {
        return ValueTask.FromResult(new OngoingTaskElasticSearchEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            MentorNode = config.MentorNode,
            TaskConnectionStatus = GetEtlTaskConnectionStatus(record, config, out var nodeES, out var elasticSearchEtlError),
            ResponsibleNode = new NodeId
            {
                NodeTag = nodeES,
                NodeUrl = clusterTopology.GetUrlFromTag(nodeES)
            },
            Error = elasticSearchEtlError
        });
    }

    protected override ValueTask<OngoingTaskSubscription> GetSubscriptionTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, SubscriptionState subscriptionState, long key)
    {
        var tag = RequestHandler.Database.WhoseTaskIsIt(record.Topology, subscriptionState, subscriptionState);
        OngoingTaskConnectionStatus connectionStatus = OngoingTaskConnectionStatus.NotActive;
        if (tag != ServerStore.NodeTag)
        {
            connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
        }
        else if (RequestHandler.Database.SubscriptionStorage.TryGetRunningSubscriptionConnectionsState(key, out var connectionsState))
        {
            connectionStatus = connectionsState.IsSubscriptionActive() ? OngoingTaskConnectionStatus.Active : OngoingTaskConnectionStatus.NotActive;
        }

        return ValueTask.FromResult(new OngoingTaskSubscription
        {
            TaskName = subscriptionState.SubscriptionName,
            TaskId = subscriptionState.SubscriptionId,
            Query = subscriptionState.Query,
            ChangeVectorForNextBatchStartingPoint = subscriptionState.ChangeVectorForNextBatchStartingPoint,
            ChangeVectorForNextBatchStartingPointPerShard = subscriptionState.ChangeVectorForNextBatchStartingPointPerShard,
            SubscriptionId = subscriptionState.SubscriptionId,
            SubscriptionName = subscriptionState.SubscriptionName,
            LastBatchAckTime = subscriptionState.LastBatchAckTime,
            Disabled = subscriptionState.Disabled,
            LastClientConnectionTime = subscriptionState.LastClientConnectionTime,
            MentorNode = subscriptionState.MentorNode,
            ResponsibleNode = new NodeId
            {
                NodeTag = tag,
                NodeUrl = clusterTopology.GetUrlFromTag(tag)
            },
            TaskConnectionStatus = connectionStatus
        });
    }

    protected override List<IncomingReplicationHandler> GetIncomingHandlers() => RequestHandler.Database.ReplicationLoader.IncomingHandlers.ToList();

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

    protected override ValueTask<OngoingTaskBackup> GetOngoingTaskBackupAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, ClusterTopology clusterTopology)
    {
        var backupStatus = RequestHandler.Database.PeriodicBackupRunner.GetBackupStatus(taskId);
        var responsibleNodeTag = RequestHandler.Database.WhoseTaskIsIt(databaseRecord.Topology, backupConfiguration, backupStatus, keepTaskOnOriginalMemberNode: true);
        var nextBackup = RequestHandler.Database.PeriodicBackupRunner.GetNextBackupDetails(databaseRecord, backupConfiguration, backupStatus, responsibleNodeTag);
        var onGoingBackup = RequestHandler.Database.PeriodicBackupRunner.OnGoingBackup(taskId);
        var backupDestinations = backupConfiguration.GetFullBackupDestinations();

        return ValueTask.FromResult(new OngoingTaskBackup
        {
            TaskId = backupConfiguration.TaskId,
            BackupType = backupConfiguration.BackupType,
            TaskName = backupConfiguration.Name,
            TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
            MentorNode = backupConfiguration.MentorNode,
            LastExecutingNodeTag = backupStatus.NodeTag,
            LastFullBackup = backupStatus.LastFullBackup,
            LastIncrementalBackup = backupStatus.LastIncrementalBackup,
            OnGoingBackup = onGoingBackup,
            NextBackup = nextBackup,
            TaskConnectionStatus = backupConfiguration.Disabled
                ? OngoingTaskConnectionStatus.NotActive
                : responsibleNodeTag == ServerStore.NodeTag
                    ? OngoingTaskConnectionStatus.Active
                    : OngoingTaskConnectionStatus.NotOnThisNode,
            ResponsibleNode = new NodeId
            {
                NodeTag = responsibleNodeTag,
                NodeUrl = clusterTopology.GetUrlFromTag(responsibleNodeTag)
            },
            BackupDestinations = backupDestinations,
            RetentionPolicy = backupConfiguration.RetentionPolicy,
            IsEncrypted = BackupTask.IsBackupEncrypted(RequestHandler.Database, backupConfiguration)
        });
    }
}
