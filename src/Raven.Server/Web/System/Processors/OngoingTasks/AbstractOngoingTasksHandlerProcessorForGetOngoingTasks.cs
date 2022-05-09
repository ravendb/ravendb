using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
using Raven.Client.Exceptions;
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<TRequestHandler, TOperationContext> : AbstractDatabaseHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public OngoingTasksResult GetOngoingTasksInternal()
    {
        var server = RequestHandler.ServerStore;
        var ongoingTasksResult = new OngoingTasksResult();
        using (server.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            ClusterTopology clusterTopology;
            DatabaseRecord databaseRecord;

            using (context.OpenReadTransaction())
            {
                databaseRecord = server.Cluster.ReadDatabase(context, RequestHandler.DatabaseName);

                if (databaseRecord == null)
                {
                    return ongoingTasksResult;
                }

                clusterTopology = server.GetClusterTopology(context);
                ongoingTasksResult.OngoingTasksList.AddRange(CollectSubscriptionTasks(context, clusterTopology, databaseRecord));
            }

            foreach (var tasks in new[]
                     {
                         CollectPullReplicationAsHubTasks(context, clusterTopology, databaseRecord),
                         CollectPullReplicationAsSinkTasks(context, clusterTopology, databaseRecord),
                         CollectExternalReplicationTasks(context, clusterTopology, databaseRecord),
                         CollectEtlTasks(context, clusterTopology, databaseRecord),
                         CollectBackupTasks(context, clusterTopology, databaseRecord)
                     })
            {
                ongoingTasksResult.OngoingTasksList.AddRange(tasks);
            }

            ongoingTasksResult.SubscriptionsCount = SubscriptionsCount;

            ongoingTasksResult.PullReplications = databaseRecord.HubPullReplications.ToList();

            return ongoingTasksResult;
        }
    }

    protected async ValueTask GetOngoingTaskInfoInternalAsync()
    {
        var (key, taskName, type) = TryGetParameters();

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            using (context.OpenReadTransaction())
            {
                var clusterTopology = ServerStore.GetClusterTopology(context);
                var record = ServerStore.Cluster.ReadDatabase(context, RequestHandler.DatabaseName);
                if (record == null)
                    throw new DatabaseDoesNotExistException(RequestHandler.DatabaseName);

                var dbTopology = record.Topology;

                switch (type)
                {
                    case OngoingTaskType.Replication:

                        var watcher = taskName != null
                            ? record.ExternalReplications.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.ExternalReplications.Find(x => x.TaskId == key);

                        if (watcher == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var externalReplicationInfo = GetExternalReplicationInfo(dbTopology, clusterTopology, watcher, record.RavenConnectionStrings);
                        await WriteResultAsync(context, externalReplicationInfo);
                        break;

                    case OngoingTaskType.PullReplicationAsHub:
                        throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");

                    case OngoingTaskType.PullReplicationAsSink:
                        var sinkReplication = record.SinkPullReplications.Find(x => x.TaskId == key);
                        if (sinkReplication == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var sinkInfo = GetPullReplicationAsSinkInfo(dbTopology, clusterTopology, record, sinkReplication);
                        await WriteResultAsync(context, sinkInfo);
                        break;

                    case OngoingTaskType.Backup:

                        var backupConfiguration = taskName != null ?
                            record.PeriodicBackups.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.PeriodicBackups?.Find(x => x.TaskId == key);

                        if (backupConfiguration == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var backupTaskInfo = GetOngoingTaskBackup(key, record, backupConfiguration, clusterTopology);
                        await WriteResultAsync(context, backupTaskInfo);
                        break;

                    case OngoingTaskType.SqlEtl:

                        var sqlEtl = taskName != null ?
                            record.SqlEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.SqlEtls?.Find(x => x.TaskId == key);

                        if (sqlEtl == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var sqlTaskInfo = GetSqlEtlTaskInfo(record, clusterTopology, sqlEtl);
                        await WriteResultAsync(context, sqlTaskInfo);
                        break;

                    case OngoingTaskType.OlapEtl:

                        var olapEtl = taskName != null ?
                            record.OlapEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.OlapEtls?.Find(x => x.TaskId == key);

                        if (olapEtl == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var olapTaskInfo = GetOlapEtlTaskInfo(record, clusterTopology, olapEtl);
                        await WriteResultAsync(context, olapTaskInfo);
                        break;

                    case OngoingTaskType.RavenEtl:

                        var ravenEtl = taskName != null ?
                            record.RavenEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.RavenEtls?.Find(x => x.TaskId == key);

                        if (ravenEtl == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var ravenTaskInfo = GetRavenEtlTaskInfo(record, clusterTopology, ravenEtl);
                        await WriteResultAsync(context, ravenTaskInfo);
                        break;

                    case OngoingTaskType.ElasticSearchEtl:

                        var elasticSearchEtl = taskName != null ?
                            record.ElasticSearchEtls.Find(x => x.Name.Equals(taskName, StringComparison.OrdinalIgnoreCase))
                            : record.ElasticSearchEtls?.Find(x => x.TaskId == key);

                        if (elasticSearchEtl == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var elasticSearchEtlInfo = GetElasticSearchEtTaskInfo(record, clusterTopology, elasticSearchEtl);
                        await WriteResultAsync(context, elasticSearchEtlInfo);
                        break;

                    case OngoingTaskType.Subscription:
                        SubscriptionState subscriptionState = null;
                        if (taskName == null)
                        {
                            subscriptionState = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateById(context, RequestHandler.DatabaseName, key);
                        }
                        else
                        {
                            try
                            {
                                subscriptionState = ServerStore.Cluster.Subscriptions.ReadSubscriptionStateByName(context, RequestHandler.DatabaseName, taskName);
                            }
                            catch (SubscriptionDoesNotExistException)
                            {
                                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                                break;
                            }
                        }

                        var subscriptionStateInfo = GetSubscriptionTaskInfo(record, clusterTopology, subscriptionState, key);
                        await WriteResultAsync(context, subscriptionStateInfo);
                        break;

                    default:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                        break;
                }
            }
        }
    }

    protected (long Key, string TaskName, OngoingTaskType Type) TryGetParameters()
    {
        AssertCanExecute();

        long key = 0;
        var taskId = RequestHandler.GetLongQueryString("key", false);
        if (taskId != null)
            key = taskId.Value;

        var taskName = RequestHandler.GetStringQueryString("taskName", false);

        if ((taskId == null) && (taskName == null))
            throw new ArgumentException($"You must specify a query string argument of either 'key' or 'name' , but none was specified.");

        var typeStr = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("type");

        if (Enum.TryParse<OngoingTaskType>(typeStr, true, out var type) == false)
            throw new ArgumentException($"Unknown task type: {type}", "type");

        return (key, taskName, type);
    }

    protected void AssertCanExecute()
    {
        if (ResourceNameValidator.IsValidResourceName(RequestHandler.DatabaseName, ServerStore.Configuration.Core.DataDirectory.FullPath, out string errorMessage) == false)
        {
            bool sharded = ShardHelper.IsShardedName(RequestHandler.DatabaseName);
            if (sharded == false)
                throw new BadRequestException(errorMessage);
        }
    }

    private async Task WriteResultAsync(JsonOperationContext context, IDynamicJson taskInfo)
    {
        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, taskInfo.ToJson());
        }
    }

    protected virtual IEnumerable<OngoingTask> CollectEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord)
    {
        foreach (var ravenEtlTask in CollectRavenEtlTasks(context, clusterTopology, databaseRecord))
        {
            yield return ravenEtlTask;
        }

        foreach (var sqlEtlTask in CollectSqlEtlTasks(context, clusterTopology, databaseRecord))
        {
            yield return sqlEtlTask;
        }

        foreach (var olapEtlTask in CollectOlapEtlTasks(context, clusterTopology, databaseRecord))
        {
            yield return olapEtlTask;
        }

        foreach (var elasticEtlTask in CollectElasticEtlTasks(context, clusterTopology, databaseRecord))
        {
            yield return elasticEtlTask;
        }
    }

    protected OngoingTaskReplication GetExternalReplicationInfo(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
        ExternalReplication watcher, Dictionary<string, RavenConnectionString> connectionStrings)
    {
        var res = GetReplicationTaskConnectionStatusAsync(databaseTopology, clusterTopology, watcher, connectionStrings, out var tag, out var connection).Result;

        return new OngoingTaskReplication
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
    }

    protected OngoingTaskPullReplicationAsSink GetPullReplicationAsSinkInfo(DatabaseTopology dbTopology, ClusterTopology clusterTopology, DatabaseRecord record, PullReplicationAsSink sinkReplication)
    {
        var sinkReplicationStatus = GetReplicationTaskConnectionStatusAsync(dbTopology, clusterTopology, sinkReplication, record.RavenConnectionStrings, out var sinkReplicationTag, out var sinkReplicationConnection).Result;

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
            TaskConnectionStatus = sinkReplicationStatus.Status,
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

        return sinkInfo;
    }

    protected OngoingTaskBackup GetOngoingTaskBackup(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration,
        ClusterTopology clusterTopology)
    {
        var backupStatus = GetBackupStatusAsync(taskId, databaseRecord, backupConfiguration, out var responsibleNodeTag, out var nextBackup, out var onGoingBackup, out var isEncrypted).Result;
        var backupDestinations = backupConfiguration.GetFullBackupDestinations();

        return new OngoingTaskBackup
        {
            TaskId = backupConfiguration.TaskId,
            BackupType = backupConfiguration.BackupType,
            TaskName = backupConfiguration.Name,
            TaskState = backupConfiguration.Disabled ? OngoingTaskState.Disabled : OngoingTaskState.Enabled,
            MentorNode = backupConfiguration.MentorNode,
            LastExecutingNodeTag = backupStatus?.NodeTag,
            LastFullBackup = backupStatus?.LastFullBackup,
            LastIncrementalBackup = backupStatus?.LastIncrementalBackup,
            OnGoingBackup = onGoingBackup,
            NextBackup = nextBackup,
            TaskConnectionStatus = backupConfiguration.Disabled
                ? OngoingTaskConnectionStatus.NotActive
                : responsibleNodeTag == ServerStore.NodeTag
                    ? OngoingTaskConnectionStatus.Active
                    : OngoingTaskConnectionStatus.NotOnThisNode,
            ResponsibleNode = new NodeId { NodeTag = responsibleNodeTag, NodeUrl = clusterTopology.GetUrlFromTag(responsibleNodeTag) },
            BackupDestinations = backupDestinations,
            RetentionPolicy = backupConfiguration.RetentionPolicy,
            IsEncrypted = isEncrypted
        };
    }

    protected OngoingTaskSqlEtlDetails GetSqlEtlTaskInfo(DatabaseRecord record, ClusterTopology clusterTopology, SqlEtlConfiguration config)
    {
        return new OngoingTaskSqlEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            MentorNode = config.MentorNode,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            TaskConnectionStatus = GetEtlTaskConnectionStatusAsync(record, config, out var sqlNode, out var sqlEtlError).Result,
            ResponsibleNode = new NodeId
            {
                NodeTag = sqlNode,
                NodeUrl = clusterTopology.GetUrlFromTag(sqlNode)
            },
            Error = sqlEtlError
        };
    }

    protected OngoingTaskOlapEtlDetails GetOlapEtlTaskInfo(DatabaseRecord record, ClusterTopology clusterTopology, OlapEtlConfiguration config)
    {
        return new OngoingTaskOlapEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            MentorNode = config.MentorNode,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            TaskConnectionStatus = GetEtlTaskConnectionStatusAsync(record, config, out var sqlNode, out var sqlEtlError).Result,
            ResponsibleNode = new NodeId { NodeTag = sqlNode, NodeUrl = clusterTopology.GetUrlFromTag(sqlNode) },
            Error = sqlEtlError
        };
    }

    protected OngoingTaskRavenEtlDetails GetRavenEtlTaskInfo(DatabaseRecord record, ClusterTopology clusterTopology, RavenEtlConfiguration config)
    {
        var process = GetProcess(config).Result;

        return new OngoingTaskRavenEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            MentorNode = config.MentorNode,
            DestinationUrl = process?.Url,
            TaskConnectionStatus = GetEtlTaskConnectionStatusAsync(record, config, out var node, out var ravenEtlError).Result,
            ResponsibleNode = new NodeId { NodeTag = node, NodeUrl = clusterTopology.GetUrlFromTag(node) },
            Error = ravenEtlError
        };
    }

    protected OngoingTaskElasticSearchEtlDetails GetElasticSearchEtTaskInfo(DatabaseRecord record, ClusterTopology clusterTopology, ElasticSearchEtlConfiguration config)
    {
        return new OngoingTaskElasticSearchEtlDetails
        {
            TaskId = config.TaskId,
            TaskName = config.Name,
            Configuration = config,
            TaskState = OngoingTasksHandler.GetEtlTaskState(config),
            MentorNode = config.MentorNode,
            TaskConnectionStatus = GetEtlTaskConnectionStatusAsync(record, config, out var nodeES, out var elasticSearchEtlError).Result,
            ResponsibleNode = new NodeId
            {
                NodeTag = nodeES,
                NodeUrl = clusterTopology.GetUrlFromTag(nodeES)
            },
            Error = elasticSearchEtlError
        };
    }

    protected OngoingTaskSubscription GetSubscriptionTaskInfo(DatabaseRecord record, ClusterTopology clusterTopology, SubscriptionState subscriptionState, long key)
    {
        var connectionStatus = GetSubscriptionConnectionStatusAsync(record, subscriptionState, key, out var tag).Result;

        return new OngoingTaskSubscription
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
            ResponsibleNode = new NodeId { NodeTag = tag, NodeUrl = clusterTopology.GetUrlFromTag(tag) },
            TaskConnectionStatus = connectionStatus
        };
    }

    protected abstract IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskBackup> CollectBackupTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskRavenEtlListView> CollectRavenEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskSqlEtlListView> CollectSqlEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskOlapEtlListView> CollectOlapEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskElasticSearchEtlListView> CollectElasticEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsHub> CollectPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract ValueTask<(string Url, OngoingTaskConnectionStatus Status)> GetReplicationTaskConnectionStatusAsync<T>(DatabaseTopology databaseTopology, ClusterTopology clusterTopology,
        T replication, Dictionary<string, RavenConnectionString> connectionStrings, out string tag, out RavenConnectionString connection)
        where T : ExternalReplicationBase;
    protected abstract ValueTask<PeriodicBackupStatus> GetBackupStatusAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration,
        out string responsibleNodeTag, out NextBackup nextBackup, out RunningBackup onGoingBackup, out bool isEncrypted);
    protected abstract ValueTask<OngoingTaskConnectionStatus> GetSubscriptionConnectionStatusAsync(DatabaseRecord record, SubscriptionState subscriptionState, long key, out string tag);
    protected abstract ValueTask<RavenEtl> GetProcess(RavenEtlConfiguration config);
    protected abstract ValueTask<OngoingTaskConnectionStatus> GetEtlTaskConnectionStatusAsync<T>(DatabaseRecord record, EtlConfiguration<T> config, out string tag,
        out string error)
        where T : ConnectionString;
    protected abstract int SubscriptionsCount { get; }
}
