using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.OngoingTasks;
using Raven.Client.Http;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Web.System.Processors.OngoingTasks;

internal abstract class AbstractOngoingTasksHandlerProcessorForGetOngoingTasks<TOperationContext> : AbstractHandlerProcessor<AbstractDatabaseRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
{
    protected AbstractOngoingTasksHandlerProcessorForGetOngoingTasks([NotNull] AbstractDatabaseRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool) : base(requestHandler, contextPool)
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
    protected abstract int SubscriptionsCount { get; }
}
