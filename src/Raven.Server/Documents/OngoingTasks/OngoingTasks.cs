using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography.X509Certificates;
using JetBrains.Annotations;
using Raven.Client;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;

namespace Raven.Server.Documents.OngoingTasks;

public class OngoingTasks : AbstractOngoingTasks
{
    private readonly DocumentDatabase _database;
    private readonly ServerStore _server;

    public OngoingTasks([NotNull] DocumentDatabase database)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _server = database.ServerStore;
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
            var backupStatus = GetBackupStatus(backupConfiguration.TaskId, databaseRecord, backupConfiguration, out var responsibleNodeTag, out var nextBackup, out var onGoingBackup, out var isEncrypted);
            var backupDestinations = backupConfiguration.GetFullBackupDestinations();

            yield return new OngoingTaskBackup
            {
                TaskId = backupConfiguration.TaskId,
                BackupType = backupConfiguration.BackupType,
                TaskName = backupConfiguration.Name,
                TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                MentorNode = backupConfiguration.MentorNode,
                PinToMentorNode = backupConfiguration.PinToMentorNode,
                LastExecutingNodeTag = backupStatus?.NodeTag,
                LastFullBackup = backupStatus?.LastFullBackup,
                LastIncrementalBackup = backupStatus?.LastIncrementalBackup,
                OnGoingBackup = onGoingBackup,
                NextBackup = nextBackup,
                TaskConnectionStatus = backupConfiguration.Disabled
                    ? OngoingTaskConnectionStatus.NotActive
                    : responsibleNodeTag == _server.NodeTag
                        ? OngoingTaskConnectionStatus.Active
                        : OngoingTaskConnectionStatus.NotOnThisNode,
                ResponsibleNode = new NodeId { NodeTag = responsibleNodeTag, NodeUrl = clusterTopology.GetUrlFromTag(responsibleNodeTag) },
                BackupDestinations = backupDestinations,
                RetentionPolicy = backupConfiguration.RetentionPolicy,
                IsEncrypted = isEncrypted
            };
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
                    PinToMentorNode = ravenEtl.PinToMentorNode,
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
                    PinToMentorNode = sqlEtl.PinToMentorNode,
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
                    PinToMentorNode = olapEtl.PinToMentorNode,
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
                    PinToMentorNode = elasticSearchEtl.PinToMentorNode,
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

    protected override IEnumerable<OngoingTaskQueueEtlListView> CollectQueueEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.QueueEtls != null)
        {
            foreach (var queueEtl in databaseRecord.QueueEtls)
            {
                databaseRecord.QueueConnectionStrings.TryGetValue(queueEtl.ConnectionStringName, out var connection);

                var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, queueEtl, out var tag, out var error);
                var taskState = OngoingTasksHandler.GetEtlTaskState(queueEtl);

                yield return new OngoingTaskQueueEtlListView
                {
                    TaskId = queueEtl.TaskId,
                    TaskName = queueEtl.Name,
                    TaskConnectionStatus = connectionStatus,
                    TaskState = taskState,
                    MentorNode = queueEtl.MentorNode,
                    PinToMentorNode = queueEtl.PinToMentorNode,
                    ResponsibleNode = new NodeId
                    {
                        NodeTag = tag,
                        NodeUrl = clusterTopology.GetUrlFromTag(tag)
                    },
                    ConnectionStringName = queueEtl.ConnectionStringName,
                    BrokerType = queueEtl.BrokerType,
                    Url = connection?.GetUrl(),
                    Error = error
                };
            }
        }
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.Topology == null)
            yield break;

        foreach (var sinkReplication in databaseRecord.SinkPullReplications)
        {
            var sinkReplicationStatus = GetReplicationTaskConnectionStatus(databaseRecord.Topology, clusterTopology, sinkReplication, databaseRecord.RavenConnectionStrings, out var sinkReplicationTag, out var sinkReplicationConnection);

            var sinkInfo = new OngoingTaskPullReplicationAsSink
            {
                TaskId = sinkReplication.TaskId,
                TaskName = sinkReplication.Name,
                ResponsibleNode = new NodeId { NodeTag = sinkReplicationTag, NodeUrl = clusterTopology.GetUrlFromTag(sinkReplicationTag) },
                ConnectionStringName = sinkReplication.ConnectionStringName,
                TaskState = sinkReplication.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = sinkReplicationConnection?.Database,
                HubName = sinkReplication.HubName,
                Mode = sinkReplication.Mode,
                DestinationUrl = sinkReplicationStatus.Url,
                TopologyDiscoveryUrls = sinkReplicationConnection?.TopologyDiscoveryUrls,
                MentorNode = sinkReplication.MentorNode,
                PinToMentorNode = sinkReplication.PinToMentorNode,
                TaskConnectionStatus = sinkReplicationStatus.Status,
                AccessName = sinkReplication.AccessName,
                AllowedHubToSinkPaths = sinkReplication.AllowedHubToSinkPaths,
                AllowedSinkToHubPaths = sinkReplication.AllowedSinkToHubPaths
            };

            if (sinkReplication.CertificateWithPrivateKey != null)
            {
                // fetch public key of certificate
                var certBytes = Convert.FromBase64String(sinkReplication.CertificateWithPrivateKey);
                var certificate = CertificateLoaderUtil.CreateCertificate(certBytes,
                    sinkReplication.CertificatePassword,
                    CertificateLoaderUtil.FlagsForExport);

                sinkInfo.CertificatePublicKey = Convert.ToBase64String(certificate.Export(X509ContentType.Cert));
            }

            yield return sinkInfo;
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

            var connectionResult = _database.ReplicationLoader.GetPullReplicationDestination(ex.TaskId, ex.Database);
            var tag = _server.NodeTag; // we can't know about pull replication tasks on other nodes.

            yield return new OngoingTaskPullReplicationAsHub
            {
                TaskId = ex.TaskId,
                TaskName = ex.Name,
                ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
                TaskState = ex.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
                DestinationDatabase = ex.Database,
                DestinationUrl = connectionResult.Url,
                MentorNode = ex.MentorNode,
                PinToMentorNode = ex.PinToMentorNode,
                TaskConnectionStatus = connectionResult.Status,
                DelayReplicationFor = ex.DelayReplicationFor
            };
        }
    }

    protected override IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var watcher in databaseRecord.ExternalReplications)
        {
            var res = GetReplicationTaskConnectionStatus(databaseRecord.Topology, clusterTopology, watcher, databaseRecord.RavenConnectionStrings, out var tag, out var connection);

            yield return new OngoingTaskReplication
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
                PinToMentorNode = watcher.PinToMentorNode,
                TaskConnectionStatus = res.Status,
                DelayReplicationFor = watcher.DelayReplicationFor
            };
        }
    }

    protected override IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var subscriptionState in _database.SubscriptionStorage.GetAllSubscriptionsFromServerStore(context))
        {
            (OngoingTaskConnectionStatus connectionStatus, string responsibleNodeTag) = _database.SubscriptionStorage.GetSubscriptionConnectionStatusAndResponsibleNode(subscriptionState.SubscriptionId, subscriptionState, databaseRecord);

            yield return OngoingTaskSubscription.From(subscriptionState, connectionStatus, clusterTopology, responsibleNodeTag);
        }
    }

    protected override OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
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

    protected override (string Url, OngoingTaskConnectionStatus Status) GetReplicationTaskConnectionStatus<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, T replication,
        Dictionary<string, RavenConnectionString> connectionStrings, out string tag, out RavenConnectionString connection)
    {
        connectionStrings.TryGetValue(replication.ConnectionStringName, out connection);
        replication.Database = connection?.Database;
        replication.ConnectionString = connection;

        var taskStatus = ReplicationLoader.GetExternalReplicationState(_server, _database.Name, replication.TaskId);
        tag = _database.WhoseTaskIsIt(databaseTopology, replication, taskStatus);

        (string Url, OngoingTaskConnectionStatus Status) result = (null, OngoingTaskConnectionStatus.None);

        if (replication is ExternalReplication)
        {
            if (tag == _server.NodeTag)
                result = _database.ReplicationLoader.GetExternalReplicationDestination(replication.TaskId);
            else
                result.Status = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        else if (replication is PullReplicationAsSink sinkReplication)
        {
            result.Status = OngoingTaskConnectionStatus.NotActive;

            if (tag == _server.NodeTag)
            {
                var handlers = GetIncomingHandlers();
                foreach (var incoming in handlers)
                {
                    if (incoming is IncomingPullReplicationHandler pullHandler &&
                        pullHandler._incomingPullReplicationParams?.Name == sinkReplication.HubName)
                    {
                        result = (incoming.ConnectionInfo.SourceUrl, OngoingTaskConnectionStatus.Active);
                        break;
                    }
                }
            }
            else
            {
                result.Status = OngoingTaskConnectionStatus.NotOnThisNode;
            }
        }

        return result;
    }

    protected override PeriodicBackupStatus GetBackupStatus(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag,
        out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted)
    {
        throw new NotImplementedException();
    }
}
