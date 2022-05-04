using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
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
using Raven.Client.Exceptions.Database;
using Raven.Client.Exceptions.Documents.Subscriptions;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Replication.Incoming;
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

    public override async ValueTask ExecuteAsync()
    {
        if (RequestHandler.HttpContext.Request.Path == $"/databases/{RequestHandler.DatabaseName}/task")
        {
            await GetOngoingTaskInfoInternal();
            return;
        }

        var result = GetOngoingTasksInternal();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            context.Write(writer, result.ToJson());
        }
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

    public async ValueTask GetOngoingTaskInfoInternal()
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

                        var taskInfo = await GetExternalReplicationInfoAsync(dbTopology, clusterTopology, watcher, record.RavenConnectionStrings);

                        await WriteResult(context, taskInfo);
                        break;

                    case OngoingTaskType.PullReplicationAsHub:
                        throw new BadRequestException("Getting task info for " + OngoingTaskType.PullReplicationAsHub + " is not supported");

                    case OngoingTaskType.PullReplicationAsSink:
                        var edge = record.SinkPullReplications.Find(x => x.TaskId == key);
                        if (edge == null)
                        {
                            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                            break;
                        }

                        var incomingHandlers = GetIncomingHandlers();

                        var sinkTaskInfo = await GetPullReplicationAsSinkInfoAsync(dbTopology, clusterTopology, record.RavenConnectionStrings, edge, incomingHandlers);

                        await WriteResult(context, sinkTaskInfo);
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

                        var backupTaskInfo = await GetOngoingTaskBackupAsync(key, record, backupConfiguration, clusterTopology);

                        await WriteResult(context, backupTaskInfo);
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

                        var sqlTaskInfo = await GetSqlEtlTaskInfoAsync(record, clusterTopology, sqlEtl);

                        await WriteResult(context, sqlTaskInfo);
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

                        var olapTaskInfo = await GetOlapEtlTaskInfoAsync(record, clusterTopology, olapEtl);

                        await WriteResult(context, olapTaskInfo);
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

                        var ravenTaskInfo = await GetRavenEtlTaskInfoAsync(record, clusterTopology, ravenEtl);
                        await WriteResult(context, ravenTaskInfo);
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

                        var elasticSearchEtlInfo = await GetElasticSearchEtTaskInfoAsync(record, clusterTopology, elasticSearchEtl);

                        await WriteResult(context, elasticSearchEtlInfo);
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

                        var subscriptionStateInfo = await GetSubscriptionTaskInfoAsync(record, clusterTopology, subscriptionState, key);

                        await WriteResult(context, subscriptionStateInfo);
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

    private async Task WriteResult(JsonOperationContext context, IDynamicJson taskInfo)
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

    protected abstract IEnumerable<OngoingTaskSubscription> CollectSubscriptionTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskBackup> CollectBackupTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskRavenEtlListView> CollectRavenEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskSqlEtlListView> CollectSqlEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskOlapEtlListView> CollectOlapEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskElasticSearchEtlListView> CollectElasticEtlTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskReplication> CollectExternalReplicationTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsSink> CollectPullReplicationAsSinkTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract IEnumerable<OngoingTaskPullReplicationAsHub> CollectPullReplicationAsHubTasks(TransactionOperationContext context, ClusterTopology clusterTopology, DatabaseRecord databaseRecord);
    protected abstract ValueTask<OngoingTaskReplication> GetExternalReplicationInfoAsync(DatabaseTopology databaseTopology, ClusterTopology clusterTopology, ExternalReplication watcher, Dictionary<string, RavenConnectionString> connectionStrings);
    protected abstract ValueTask<OngoingTaskPullReplicationAsSink> GetPullReplicationAsSinkInfoAsync(DatabaseTopology dbTopology, ClusterTopology clusterTopology, Dictionary<string, RavenConnectionString> connectionStrings, PullReplicationAsSink sinkReplication, List<IncomingReplicationHandler> handlers);
    protected abstract ValueTask<OngoingTaskBackup> GetOngoingTaskBackupAsync(long taskId, DatabaseRecord databaseRecord, PeriodicBackupConfiguration backupConfiguration, ClusterTopology clusterTopology);
    protected abstract ValueTask<OngoingTaskSqlEtlDetails> GetSqlEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, SqlEtlConfiguration config);
    protected abstract ValueTask<OngoingTaskOlapEtlDetails> GetOlapEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, OlapEtlConfiguration config);
    protected abstract ValueTask<OngoingTaskRavenEtlDetails> GetRavenEtlTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, RavenEtlConfiguration config);
    protected abstract ValueTask<OngoingTaskElasticSearchEtlDetails> GetElasticSearchEtTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, ElasticSearchEtlConfiguration config);
    protected abstract ValueTask<OngoingTaskSubscription> GetSubscriptionTaskInfoAsync(DatabaseRecord record, ClusterTopology clusterTopology, SubscriptionState subscriptionState, long key);
    protected abstract List<IncomingReplicationHandler> GetIncomingHandlers();
    protected abstract int SubscriptionsCount { get; }
}
