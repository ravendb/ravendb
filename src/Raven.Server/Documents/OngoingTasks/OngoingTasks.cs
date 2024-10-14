using System;
using System.Collections.Generic;
using System.Linq;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Documents.QueueSink;
using Raven.Server.Documents.Replication;
using Raven.Server.Documents.Replication.Incoming;
using Raven.Server.Documents.Replication.Outgoing;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.OngoingTasks;

public sealed class OngoingTasks : AbstractOngoingTasks<SubscriptionConnectionsState>
{
    private readonly DocumentDatabase _database;
    private readonly ServerStore _server;

    public OngoingTasks([NotNull] DocumentDatabase database)
        : base(database.ServerStore, database.SubscriptionStorage)
    {
        _database = database ?? throw new ArgumentNullException(nameof(database));
        _server = database.ServerStore;
    }

    protected override DatabaseTopology GetDatabaseTopology(DatabaseRecord databaseRecord) => databaseRecord.Topology;

    protected override string GetDestinationUrlForRavenEtl(string name)
    {
        var process = _database.EtlLoader.Processes.OfType<RavenEtl>().FirstOrDefault(x => x.ConfigurationName == name);
        return process?.Url;
    }

    protected override IEnumerable<OngoingTaskPullReplicationAsHub> GetPullReplicationAsHubTasks(JsonOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
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

    protected override OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
    {
        var connectionStatus = OngoingTaskConnectionStatus.None;
        error = null;

        var processState = EtlLoader.GetProcessState(config.Transforms, _database, config.Name);

        tag = OngoingTasksUtils.WhoseTaskIsIt(_server, record.Topology, config, processState, _database.NotificationCenter);

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

    protected override OngoingTaskConnectionStatus GetQueueSinkTaskConnectionStatus(DatabaseRecord record, QueueSinkConfiguration config,
        out string tag, out string error)
    {
        var connectionStatus = OngoingTaskConnectionStatus.None;
        error = null;

        var processState = QueueSinkLoader.GetProcessState(config.Scripts, _database, config.Name);

        tag = OngoingTasksUtils.WhoseTaskIsIt(_server, record.Topology, config, processState, _database.NotificationCenter);

        if (tag == _server.NodeTag)
        {
            var process = _database.QueueSinkLoader.Processes.FirstOrDefault(x => x.Configuration.Name == config.Name);

            if (process != null)
                connectionStatus = process.GetConnectionStatus();
            else
            {
                if (config.Disabled)
                    connectionStatus = OngoingTaskConnectionStatus.NotActive;
                else
                    error = $"Queue Sink process '{config.Name}' was not found.";
            }
        }
        else
        {
            connectionStatus = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        return connectionStatus;
    }

    protected override (string Url, OngoingTaskConnectionStatus Status) GetReplicationTaskConnectionStatus<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, T replication, 
        Dictionary<string, RavenConnectionString> connectionStrings, out ExternalReplicationState replicationState, out string responsibleNodeTag, out RavenConnectionString connection)
    {
        connectionStrings.TryGetValue(replication.ConnectionStringName, out connection);
        replication.Database = connection?.Database;
        replication.ConnectionString = connection;

        replicationState = ReplicationLoader.GetExternalReplicationState(_server, _database.Name, replication.TaskId);
        responsibleNodeTag = OngoingTasksUtils.WhoseTaskIsIt(_server, databaseTopology, replication, replicationState, _database.NotificationCenter);

        (string Url, OngoingTaskConnectionStatus Status) result = (null, OngoingTaskConnectionStatus.None);

        if (replication is ExternalReplication)
        {
            if (responsibleNodeTag == _server.NodeTag)
            {
                result = _database.ReplicationLoader.GetExternalReplicationDestination(replication.TaskId, out var fromToString);
                replicationState.FromToString = fromToString;
            }
            else
                result.Status = OngoingTaskConnectionStatus.NotOnThisNode;
        }

        else if (replication is PullReplicationAsSink sinkReplication)
        {
            result.Status = OngoingTaskConnectionStatus.NotActive;

            if (responsibleNodeTag == _server.NodeTag)
            {
                var handlers = GetIncomingReplicationHandlers();
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

    protected override PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag,
        out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted)
    {
        var backupStatus = _database.PeriodicBackupRunner.GetBackupStatus(taskId);
        nextBackup = _database.PeriodicBackupRunner.GetNextBackupDetails(backupConfiguration, backupStatus, out responsibleNodeTag);
        onGoingBackup = _database.PeriodicBackupRunner.OnGoingBackup(taskId);
        isEncrypted = BackupTask.IsBackupEncrypted(_database, backupConfiguration);

        return backupStatus;
    }

    private List<IAbstractIncomingReplicationHandler> GetIncomingReplicationHandlers() => _database.ReplicationLoader.IncomingHandlers.ToList();
}
