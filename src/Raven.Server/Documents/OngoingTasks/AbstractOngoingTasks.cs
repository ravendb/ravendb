using System;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Client.Documents.Operations.ETL.ElasticSearch;
using Raven.Client.Documents.Operations.ETL.OLAP;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Client.Documents.Operations.ETL.Snowflake;
using Raven.Client.Documents.Operations.ETL.SQL;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Documents.Operations.QueueSink;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Exceptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents.Subscriptions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;

namespace Raven.Server.Documents.OngoingTasks;

public abstract class AbstractOngoingTasks<TSubscriptionConnectionsState>
    where TSubscriptionConnectionsState : AbstractSubscriptionConnectionsState
{
    private readonly ServerStore _server;
    private readonly AbstractSubscriptionStorage<TSubscriptionConnectionsState> _subscriptionStorage;

    protected AbstractOngoingTasks([NotNull] ServerStore server, [NotNull] AbstractSubscriptionStorage<TSubscriptionConnectionsState> subscriptionStorage)
    {
        _server = server ?? throw new ArgumentNullException(nameof(server));
        _subscriptionStorage = subscriptionStorage ?? throw new ArgumentNullException(nameof(subscriptionStorage));
    }

    protected abstract DatabaseTopology GetDatabaseTopology(DatabaseRecord databaseRecord);

    protected abstract string GetDestinationUrlForRavenEtl(string name);

    private IEnumerable<OngoingTaskBackup> GetBackupTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.PeriodicBackups == null || databaseRecord.PeriodicBackups.Count == 0)
            yield break;

        foreach (var backupConfiguration in databaseRecord.PeriodicBackups)
            yield return CreateBackupTaskInfo(clusterTopology, backupConfiguration);
    }

    private IEnumerable<OngoingTaskRavenEtl> GetRavenEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.RavenEtls == null || databaseRecord.RavenEtls.Count == 0)
            yield break;

        foreach (var ravenEtl in databaseRecord.RavenEtls)
            yield return CreateRavenEtlTaskInfo(clusterTopology, databaseRecord, ravenEtl);
    }

    private IEnumerable<OngoingTaskSqlEtl> GetSqlEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.SqlEtls == null || databaseRecord.SqlEtls.Count == 0)
            yield break;

        foreach (var sqlEtl in databaseRecord.SqlEtls)
            yield return CreateSqlEtlTaskInfo(clusterTopology, databaseRecord, sqlEtl);
    }

    private IEnumerable<OngoingTaskOlapEtl> GetOlapEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.OlapEtls == null || databaseRecord.OlapEtls.Count == 0)
            yield break;

        foreach (var olapEtl in databaseRecord.OlapEtls)
            yield return CreateOlapEtlTaskInfo(clusterTopology, databaseRecord, olapEtl);
    }

    private IEnumerable<OngoingTaskElasticSearchEtl> GetElasticEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.ElasticSearchEtls == null || databaseRecord.ElasticSearchEtls.Count == 0)
            yield break;

        foreach (var elasticSearchEtl in databaseRecord.ElasticSearchEtls)
            yield return CreateElasticSearchEtlTaskInfo(clusterTopology, databaseRecord, elasticSearchEtl);
    }

    private IEnumerable<OngoingTaskQueueEtl> GetQueueEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.QueueEtls == null || databaseRecord.QueueEtls.Count == 0)
            yield break;

        foreach (var queueEtl in databaseRecord.QueueEtls)
            yield return CreateQueueEtlTaskInfo(clusterTopology, databaseRecord, queueEtl);
    }
    
    private IEnumerable<OngoingTaskSnowflakeEtl> GetSnowflakeEtlTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.SnowflakeEtls == null || databaseRecord.SnowflakeEtls.Count == 0)
            yield break;

        foreach (var snowflakeEtl in databaseRecord.SnowflakeEtls)
            yield return CreateSnowflakeEtlTaskInfo(clusterTopology, databaseRecord, snowflakeEtl);
    }

    private IEnumerable<OngoingTaskPullReplicationAsSink> GetPullReplicationAsSinkTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var sinkReplication in databaseRecord.SinkPullReplications)
            yield return CreatePullReplicationAsSinkTaskInfo(clusterTopology, databaseRecord, sinkReplication);
    }

    protected abstract IEnumerable<OngoingTaskPullReplicationAsHub> GetPullReplicationAsHubTasks(JsonOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);

    private IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var watcher in databaseRecord.ExternalReplications)
            yield return CreateExternalReplicationTaskInfo(clusterTopology, databaseRecord, watcher);
    }

    private IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(ClusterOperationContext context, ClusterTopology clusterTopology)
    {
        foreach (var subscriptionState in _subscriptionStorage.GetAllSubscriptionsFromServerStore(context))
            yield return CreateSubscriptionTaskInfo(context, clusterTopology, subscriptionState);
    }
    
    private IEnumerable<OngoingTaskQueueSink> GetQueueSinkTasks(ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        if (databaseRecord.QueueSinks == null || databaseRecord.QueueSinks.Count == 0)
            yield break;

        foreach (var queueSink in databaseRecord.QueueSinks)
            yield return CreateQueueSinkTaskInfo(clusterTopology, databaseRecord, queueSink);
    }

    public IEnumerable<OngoingTask> GetAllTasks(ClusterOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var task in CollectSubscriptionTasks(context, clusterTopology))
            yield return task;

        foreach (var task in GetBackupTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetRavenEtlTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetSqlEtlTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetOlapEtlTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetElasticEtlTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetQueueEtlTasks(clusterTopology, databaseRecord))
            yield return task;
        
        foreach (var task in GetSnowflakeEtlTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetPullReplicationAsSinkTasks(clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in GetPullReplicationAsHubTasks(context, clusterTopology, databaseRecord))
            yield return task;

        foreach (var task in CollectExternalReplicationTasks(clusterTopology, databaseRecord))
            yield return task;
        
        foreach (var task in GetQueueSinkTasks(clusterTopology, databaseRecord))
            yield return task;
    }

    public OngoingTask GetTask(ClusterOperationContext context, long? taskId, string taskName, OngoingTaskType taskType, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        switch (taskType)
        {
            case OngoingTaskType.Replication:
                var watcher = taskName != null
                    ? databaseRecord.ExternalReplications.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.ExternalReplications.Find(x => x.TaskId == taskId);

                if (watcher == null)
                    return null;

                return CreateExternalReplicationTaskInfo(clusterTopology, databaseRecord, watcher);

            case OngoingTaskType.PullReplicationAsHub:
                throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");

            case OngoingTaskType.PullReplicationAsSink:
                var sinkReplication = databaseRecord.SinkPullReplications.Find(x => x.TaskId == taskId);

                if (sinkReplication == null)
                    return null;

                return CreatePullReplicationAsSinkTaskInfo(clusterTopology, databaseRecord, sinkReplication);

            case OngoingTaskType.Backup:

                var backupConfiguration = taskName != null ?
                    databaseRecord.PeriodicBackups.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.PeriodicBackups?.Find(x => x.TaskId == taskId);

                if (backupConfiguration == null)
                    return null;

                return CreateBackupTaskInfo(clusterTopology, backupConfiguration);

            case OngoingTaskType.SqlEtl:

                var sqlEtl = taskName != null ?
                    databaseRecord.SqlEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.SqlEtls?.Find(x => x.TaskId == taskId);

                if (sqlEtl == null)
                    return null;

                return CreateSqlEtlTaskInfo(clusterTopology, databaseRecord, sqlEtl);

            case OngoingTaskType.OlapEtl:

                var olapEtl = taskName != null
                    ? databaseRecord.OlapEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.OlapEtls?.Find(x => x.TaskId == taskId);

                if (olapEtl == null)
                    return null;

                return CreateOlapEtlTaskInfo(clusterTopology, databaseRecord, olapEtl);

            case OngoingTaskType.QueueEtl:

                var queueEtl = taskName != null
                    ? databaseRecord.QueueEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.QueueEtls?.Find(x => x.TaskId == taskId);

                if (queueEtl == null)
                    return null;

                return CreateQueueEtlTaskInfo(clusterTopology, databaseRecord, queueEtl);

            case OngoingTaskType.RavenEtl:

                var ravenEtl = taskName != null ?
                    databaseRecord.RavenEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.RavenEtls?.Find(x => x.TaskId == taskId);

                if (ravenEtl == null)
                    return null;

                return CreateRavenEtlTaskInfo(clusterTopology, databaseRecord, ravenEtl);

            case OngoingTaskType.ElasticSearchEtl:

                var elasticSearchEtl = taskName != null ?
                    databaseRecord.ElasticSearchEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.ElasticSearchEtls?.Find(x => x.TaskId == taskId);

                if (elasticSearchEtl == null)
                    return null;

                return CreateElasticSearchEtlTaskInfo(clusterTopology, databaseRecord, elasticSearchEtl);
            
            
            case OngoingTaskType.SnowflakeEtl:

                var snowflakeEtl = taskName != null ?
                    databaseRecord.SnowflakeEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.SnowflakeEtls?.Find(x => x.TaskId == taskId);

                if (snowflakeEtl == null)
                    return null;

                return CreateSnowflakeEtlTaskInfo(clusterTopology, databaseRecord, snowflakeEtl);

            case OngoingTaskType.Subscription:

                var subscriptionState = taskName != null
                    ? _subscriptionStorage.GetSubscriptionByName(context, taskName)
                    : _subscriptionStorage.GetSubscriptionById(context, taskId.Value);

                return CreateSubscriptionTaskInfo(context, clusterTopology, subscriptionState);
            case OngoingTaskType.QueueSink:

                var queueSink = taskName != null
                    ? databaseRecord.QueueSinks.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                    : databaseRecord.QueueSinks?.Find(x => x.TaskId == taskId);

                if (queueSink == null)
                    return null;

                return CreateQueueSinkTaskInfo(clusterTopology, databaseRecord, queueSink);
            default:
                return null;
        }
    }

    protected abstract OngoingTaskConnectionStatus GetEtlTaskConnectionStatus<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag, out string error)
        where T : ConnectionString;
    
    protected abstract OngoingTaskConnectionStatus GetQueueSinkTaskConnectionStatus(DatabaseRecord record, QueueSinkConfiguration config, out string tag, out string error);

    protected abstract (string Url, OngoingTaskConnectionStatus Status) GetReplicationTaskConnectionStatus<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, T replication, Dictionary<string, RavenConnectionString> connectionStrings, out string responsibleNodeTag, out RavenConnectionString connection)
        where T : ExternalReplicationBase;

    protected abstract PeriodicBackupStatus GetBackupStatus(long taskId, PeriodicBackupConfiguration backupConfiguration, out string responsibleNodeTag, out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted);

    private OngoingTaskReplication CreateExternalReplicationTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, ExternalReplication watcher)
    {
        var res = GetReplicationTaskConnectionStatus(GetDatabaseTopology(databaseRecord), clusterTopology, watcher, databaseRecord.RavenConnectionStrings, out var tag, out var connection);

        NodeId responsibleNode = null;
        if (tag != null)
            responsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) };

        return new OngoingTaskReplication
        {
            TaskId = watcher.TaskId,
            TaskName = watcher.Name,
            ResponsibleNode = responsibleNode,
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

    private OngoingTaskSubscription CreateSubscriptionTaskInfo(ClusterOperationContext context, ClusterTopology clusterTopology, SubscriptionState subscriptionState)
    {
        (OngoingTaskConnectionStatus connectionStatus, string responsibleNodeTag) = _subscriptionStorage.GetSubscriptionConnectionStatusAndResponsibleNode(context, subscriptionState.SubscriptionId, subscriptionState);

        return OngoingTaskSubscription.From(subscriptionState, connectionStatus, clusterTopology, responsibleNodeTag);
    }

    private OngoingTaskBackup CreateBackupTaskInfo(ClusterTopology clusterTopology, PeriodicBackupConfiguration backupConfiguration)
    {
        var backupStatus = GetBackupStatus(backupConfiguration.TaskId, backupConfiguration, out var responsibleNodeTag, out var nextBackup,
            out var onGoingBackup, out var isEncrypted);
        var backupDestinations = backupConfiguration.GetFullBackupDestinations();

        return new OngoingTaskBackup
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

    private OngoingTaskRavenEtl CreateRavenEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, RavenEtlConfiguration ravenEtl)
    {
        var taskState = OngoingTasksHandler.GetEtlTaskState(ravenEtl);

        databaseRecord.RavenConnectionStrings.TryGetValue(ravenEtl.ConnectionStringName, out var connection);

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, ravenEtl, out var tag, out var error);

        return new OngoingTaskRavenEtl
        {
            TaskId = ravenEtl.TaskId,
            TaskName = ravenEtl.Name,
            TaskState = taskState,
            MentorNode = ravenEtl.MentorNode,
            PinToMentorNode = ravenEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            DestinationUrl = GetDestinationUrlForRavenEtl(ravenEtl.Name),
            TaskConnectionStatus = connectionStatus,
            DestinationDatabase = connection?.Database,
            ConnectionStringName = ravenEtl.ConnectionStringName,
            TopologyDiscoveryUrls = connection?.TopologyDiscoveryUrls,
            Error = error,
            Configuration = ravenEtl
        };
    }

    private OngoingTaskSqlEtl CreateSqlEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, SqlEtlConfiguration sqlEtl)
    {
        string database = null;
        string server = null;

        if (databaseRecord.SqlConnectionStrings.TryGetValue(sqlEtl.ConnectionStringName, out var sqlConnection))
        {
            (database, server) = SqlConnectionStringParser.GetDatabaseAndServerFromConnectionString(sqlConnection.FactoryName, sqlConnection.ConnectionString);
        }

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, sqlEtl, out var tag, out var error);

        var taskState = OngoingTasksHandler.GetEtlTaskState(sqlEtl);

        return new OngoingTaskSqlEtl
        {
            TaskId = sqlEtl.TaskId,
            TaskName = sqlEtl.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = sqlEtl.MentorNode,
            PinToMentorNode = sqlEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            DestinationServer = server,
            DestinationDatabase = database,
            ConnectionStringDefined = sqlConnection != null,
            ConnectionStringName = sqlEtl.ConnectionStringName,
            Error = error,
            Configuration = sqlEtl
        };
    }

    private OngoingTaskOlapEtl CreateOlapEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, OlapEtlConfiguration olapEtl)
    {
        string destination = default;
        if (databaseRecord.OlapConnectionStrings.TryGetValue(olapEtl.ConnectionStringName, out var olapConnection))
        {
            destination = olapConnection.GetDestination();
        }

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, olapEtl, out var tag, out var error);

        var taskState = OngoingTasksHandler.GetEtlTaskState(olapEtl);

        return new OngoingTaskOlapEtl
        {
            TaskId = olapEtl.TaskId,
            TaskName = olapEtl.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = olapEtl.MentorNode,
            PinToMentorNode = olapEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            ConnectionStringName = olapEtl.ConnectionStringName,
            Destination = destination,
            Error = error,
            Configuration = olapEtl
        };
    }

    private OngoingTaskElasticSearchEtl CreateElasticSearchEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, ElasticSearchEtlConfiguration elasticSearchEtl)
    {
        databaseRecord.ElasticSearchConnectionStrings.TryGetValue(elasticSearchEtl.ConnectionStringName, out var connection);

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, elasticSearchEtl, out var tag, out var error);
        var taskState = OngoingTasksHandler.GetEtlTaskState(elasticSearchEtl);

        return new OngoingTaskElasticSearchEtl
        {
            TaskId = elasticSearchEtl.TaskId,
            TaskName = elasticSearchEtl.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = elasticSearchEtl.MentorNode,
            PinToMentorNode = elasticSearchEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            ConnectionStringName = elasticSearchEtl.ConnectionStringName,
            NodesUrls = connection?.Nodes,
            Error = error,
            Configuration = elasticSearchEtl
        };
    }

    private OngoingTaskQueueEtl CreateQueueEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, QueueEtlConfiguration queueEtl)
    {
        databaseRecord.QueueConnectionStrings.TryGetValue(queueEtl.ConnectionStringName, out var connection);

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, queueEtl, out var tag, out var error);
        var taskState = OngoingTasksHandler.GetEtlTaskState(queueEtl);

        return new OngoingTaskQueueEtl
        {
            TaskId = queueEtl.TaskId,
            TaskName = queueEtl.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = queueEtl.MentorNode,
            PinToMentorNode = queueEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            ConnectionStringName = queueEtl.ConnectionStringName,
            BrokerType = queueEtl.BrokerType,
            Url = connection?.GetUrl(),
            Error = error,
            Configuration = queueEtl
        };
    }
    
    
    private OngoingTaskSnowflakeEtl CreateSnowflakeEtlTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, SnowflakeEtlConfiguration snowflakeEtl)
    {
        string connectionString = null;

        if (databaseRecord.SnowflakeConnectionStrings.TryGetValue(snowflakeEtl.ConnectionStringName, out var snowflakeConnection))
        {
            connectionString = snowflakeConnection.ConnectionString;
        }

        var connectionStatus = GetEtlTaskConnectionStatus(databaseRecord, snowflakeEtl, out var tag, out var error);

        var taskState = OngoingTasksHandler.GetEtlTaskState(snowflakeEtl);

        return new OngoingTaskSnowflakeEtl
        {
            TaskId = snowflakeEtl.TaskId,
            TaskName = snowflakeEtl.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = snowflakeEtl.MentorNode,
            PinToMentorNode = snowflakeEtl.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            ConnectionStringName = snowflakeEtl.ConnectionStringName,
            ConnectionString = connectionString,
            Error = error,
            Configuration = snowflakeEtl
        };
    }

    private OngoingTaskPullReplicationAsSink CreatePullReplicationAsSinkTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, PullReplicationAsSink sinkReplication)
    {
        var sinkReplicationStatus = GetReplicationTaskConnectionStatus(GetDatabaseTopology(databaseRecord), clusterTopology, sinkReplication, databaseRecord.RavenConnectionStrings, out var sinkReplicationTag, out var sinkReplicationConnection);

        NodeId responsibleNode = null;
        if (sinkReplicationTag != null)
            responsibleNode = new NodeId { NodeTag = sinkReplicationTag, NodeUrl = clusterTopology.GetUrlFromTag(sinkReplicationTag) };

        var sinkInfo = new OngoingTaskPullReplicationAsSink
        {
            TaskId = sinkReplication.TaskId,
            TaskName = sinkReplication.Name,
            ResponsibleNode = responsibleNode,
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

        return sinkInfo;
    }
    
    private OngoingTaskQueueSink CreateQueueSinkTaskInfo(ClusterTopology clusterTopology, DatabaseRecord databaseRecord, QueueSinkConfiguration queueSink)
    {
        databaseRecord.QueueConnectionStrings.TryGetValue(queueSink.ConnectionStringName, out var connection);

        var connectionStatus = GetQueueSinkTaskConnectionStatus(databaseRecord, queueSink, out var tag, out var error);
        var taskState = OngoingTasksHandler.GetQueueSinkTaskState(queueSink);

        return new OngoingTaskQueueSink
        {
            TaskId = queueSink.TaskId,
            TaskName = queueSink.Name,
            TaskConnectionStatus = connectionStatus,
            TaskState = taskState,
            MentorNode = queueSink.MentorNode,
            PinToMentorNode = queueSink.PinToMentorNode,
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            BrokerType = queueSink.BrokerType,
            Error = error,
            Configuration = queueSink,
            ConnectionStringName = queueSink.ConnectionStringName,
            Url = connection?.GetUrl()
        };
    }
}
